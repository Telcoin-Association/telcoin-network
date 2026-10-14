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
    gas_accumulator::RewardsCounter, max_batch_size, AuthorityIdentifier, Batch, BlockHash,
    BlockNumHash, BlsPublicKey, CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch, EpochRecord, Round, B256,
    MAX_GC_DEPTH, MAX_HEADER_NUM_OF_BATCHES,
};
use tokio::{
    io::{AsyncRead, BufReader},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
};
use tracing::{debug, error, info, warn};

use crate::{
    archive::{
        data_file::{create_dir_synced, fsync_directory},
        digest_index::HdxIndex,
        error::{
            fetch::FetchError,
            load_header::LoadHeaderError,
            open::OpenError::{self, DataFileOpen},
        },
        fxhasher::FxHasher,
        index::Index as _,
        pack::{DataHeader, Pack, PackCompression, DATA_HEADER_BYTES},
        pack_iter::AsyncPackIter,
        position_index::index::{PosIndexValue, PositionIndex},
    },
    pack_validate::CorruptionKind,
};

/// Current version for new pack files.
///
/// v2 is byte-identical to v1 on disk (header-first record layout); the sole difference is that a
/// v2 file carries the 8-byte clean-close sentinel the data-file layer appends on a clean shutdown,
/// whereas v0 and v1 predate the sentinel and never have one. Writing new packs as v2 is what lets
/// a missing sentinel be read as a genuine "not cleanly closed" signal: a pre-sentinel pack (v0/v1)
/// is recognized by its version and trusted via the length / WAL cross-checks instead (see
/// `SENTINEL_MIN_VERSION` and `Inner::files_consistent`).
pub const PACK_VERSION: u16 = 2;

/// First pack version whose files carry the clean-close sentinel.
///
/// A file whose on-disk version is below this predates the sentinel (the buffered backend never
/// wrote one), so the *absence* of a sentinel is normal and must not be read as an unclean
/// shutdown; such packs are validated by the cross-file length checks and, on the writable door, by
/// WAL replay — exactly how pre-mmap `main` treated them. Kept distinct from [`PACK_VERSION`] so a
/// later version bump cannot silently drop the sentinel gate for v2.
pub const SENTINEL_MIN_VERSION: u16 = 2;

/// Metadata for an Epoch.  Should always be the first record in a consensus pack.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
pub struct EpochMeta {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    ///
    /// The full committee is stored, not just the BLS keys, so `ConsensusOutput` can be
    /// reconstructed from the pack. Only the BLS key set is authenticated (by `verify_epoch_meta`
    /// on import, and re-checked by `open_append` on reopen). In a pack imported from a peer every
    /// other field is the serving peer's copy, may differ from the local committee for the same
    /// epoch, and must not feed consensus-critical logic.
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

impl EpochMeta {
    /// Compares `on_disk` with this chain-derived meta on exactly the fields
    /// [`verify_epoch_meta`] authenticates, and describes the first one that differs.
    ///
    /// The checks run in `verify_epoch_meta`'s order: the epoch, the on-disk committee's own epoch,
    /// the start consensus number, the genesis execution state, the genesis consensus header, and
    /// the committee's BLS key set. Nothing else in the committee is compared: a pack imported from
    /// a peer stores the serving peer's copy of the remaining fields, which import cannot
    /// authenticate, so comparing them would refuse a correctly imported epoch whose committee
    /// differs from the local one only where no syncing node can check it.
    ///
    /// Returns `None` when every authenticated field matches. Otherwise the message names the
    /// differing field and carries only that field's expected (chain-derived) and actual (on-disk)
    /// values; a key-set difference lists the keys missing from and added to the on-disk committee.
    pub(crate) fn authenticated_mismatch(&self, on_disk: &EpochMeta) -> Option<String> {
        if self.epoch != on_disk.epoch {
            return Some(format!("epoch: expected {}, got {}", self.epoch, on_disk.epoch));
        }
        if on_disk.committee.epoch() != on_disk.epoch {
            return Some(format!(
                "committee epoch: expected {}, got {}",
                on_disk.epoch,
                on_disk.committee.epoch()
            ));
        }
        if self.start_consensus_number != on_disk.start_consensus_number {
            return Some(format!(
                "start_consensus_number: expected {}, got {}",
                self.start_consensus_number, on_disk.start_consensus_number
            ));
        }
        if self.genesis_exec_state != on_disk.genesis_exec_state {
            return Some(format!(
                "genesis_exec_state: expected {:?}, got {:?}",
                self.genesis_exec_state, on_disk.genesis_exec_state
            ));
        }
        if self.genesis_consensus != on_disk.genesis_consensus {
            return Some(format!(
                "genesis_consensus: expected {:?}, got {:?}",
                self.genesis_consensus, on_disk.genesis_consensus
            ));
        }
        let expected_keys = self.committee.bls_keys();
        let on_disk_keys = on_disk.committee.bls_keys();
        if expected_keys != on_disk_keys {
            let missing: Vec<_> = expected_keys.difference(&on_disk_keys).collect();
            let added: Vec<_> = on_disk_keys.difference(&expected_keys).collect();
            return Some(format!("committee bls keys: missing {missing:?}, added {added:?}"));
        }
        None
    }
}

/// Descriminant type for records in a Consensus Pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PackRecord {
    /// The epoch metadata record; always the first record in a pack.
    EpochMeta(EpochMeta),
    /// A batch belonging to the most recently written consensus output.
    Batch(Batch),
    /// A consensus header, followed by the batch records its sub-dag owns.
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
    LatestConsensusNumber(oneshot::Sender<u64>),
    Shutdown,
    AsyncShutdown(oneshot::Sender<()>),
    // Flush the write buffer to the data file WITHOUT fsync, so freshly appended bytes
    /// become visible to other file handles on the same file (visibility, not durability).
    FlushData(oneshot::Sender<Result<(), PackError>>),
    /// Read the logical data length (`end`) of the pack — the number of real record bytes,
    /// ignoring the mmap capacity padding. A consumer copying the raw file bounds its read to
    /// this so it never captures the trailing zero padding (or a later append).
    DataFileLen(oneshot::Sender<u64>),
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
    // An async shutdown stashes its confirmation here so it can be sent AFTER the clean-close
    // below.
    // Note: code in this thread should NEVER panic. If it does, unwinding drops `Inner`, but the
    // data file's `Drop` does not seal while panicking: a mid-save panic leaves the pack
    // unsealed, so the next open runs recovery and drops the incomplete output instead of
    // trusting it.
    let mut async_confirm: Option<oneshot::Sender<()>> = None;
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
            PackMessage::LatestConsensusNumber(tx) => {
                let _ = tx.send(inner.latest_consensus_number());
            }
            PackMessage::Shutdown => break,
            PackMessage::AsyncShutdown(tx) => {
                // Confirm AFTER the clean-close below (not here) so `close().await` returns only
                // once the pack is fully sealed: data committed, indexes synced, sentinels written.
                async_confirm = Some(tx);
                break;
            }
            PackMessage::FlushData(tx) => {
                let _ = tx.send(inner.flush_data());
            }
            PackMessage::DataFileLen(tx) => {
                let _ = tx.send(inner.data.file_len());
            }
        }
    }
    // Clean-close: dropping `inner` commits the data, ordered-syncs the indexes, and writes the
    // clean-close sentinels. Do it before confirming an async shutdown; it also runs for the sync
    // `Shutdown` and channel-closed paths (the sync `Drop`'s `join()` waits on this return).
    drop(inner);
    if let Some(tx) = async_confirm {
        let _ = tx.send(());
    }
}

impl Drop for ConsensusPack {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handle) == 1 {
            // If we are the last ConsensusPack then shutdown thread and wait for it to persist and
            // exit. Reaching this with a live handle means close() was NOT used: a correct
            // close().await already took the handle, so the block below is skipped. Drop is the
            // safety net; the proper async path is close().await.
            if let Some(handle) = self.handle.lock().take() {
                if self.tx.is_closed() {
                    // The actor already exited (only a panic ends it while a handle still holds
                    // its join handle): there is nothing left to seal, and `close()` could not have
                    // done better. Reap the finished thread and report why it ended.
                    if let Err(e) = handle.join() {
                        error!(target: "consensus_pack", ?e, epoch = self.epoch, "consensus pack thread had panicked");
                    }
                    return;
                }
                warn!(target: "consensus_pack", "ConsensusPack dropped without calling close(); sealing as a fallback");
                if self.tx.try_send(PackMessage::Shutdown).is_err() {
                    // Full bounded channel — detach. The actor clean-closes when the last Sender
                    // drops; only the synchronous "sealed on return" wait is lost, and only on this
                    // misuse path.
                    error!(target: "consensus_pack", "Failed to send shutdown message to ConsensusPack (should be using close())");
                    return;
                }
                let join = move || {
                    if let Err(e) = handle.join() {
                        error!(target: "consensus_pack", ?e, "Failed to join consensus pack thread");
                    }
                };
                // Never block a multi-threaded runtime worker on the ~60-75ms clean-close fsyncs:
                // offload the join to the blocking pool. On a current-thread runtime (nothing else
                // to starve) or no runtime, a synchronous join keeps "sealed on
                // return" for callers/tests that drop then immediately reopen.
                // `close().await` is still the intended path.
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

/// Outcome of [`ConsensusPack::repair_epoch`] for one epoch's consensus pack (data + indexes).
#[derive(Debug)]
pub enum EpochRepair {
    /// The epoch opened read-only cleanly; nothing was wrong and nothing was written.
    Healthy,
    /// The epoch was damaged in a repairable way and (with `apply`) was repaired and re-sealed.
    /// The string describes what was done.
    Repaired(String),
    /// The epoch is damaged in a repairable way but this was a dry run (`apply == false`); no
    /// write happened. The string describes what a repair would do.
    WouldRepair(String),
    /// The epoch is damaged in a way local repair cannot fix (a torn/corrupt epoch-meta, or
    /// mid-log data corruption): the data is lost and the epoch must be re-synced from peers.
    /// Nothing was written. The string is the operator-facing reason.
    Unrepairable(String),
}

impl EpochRepair {
    /// True when this outcome changed the on-disk pack (a real repair was applied).
    pub fn was_repaired(&self) -> bool {
        matches!(self, EpochRepair::Repaired(_))
    }
}

/// Outcome of [`ConsensusPack::migrate_epoch`] for one epoch's consensus pack: migrating a
/// pre-v2 (v0 batches-first / v1 header-first) pack up to the current v2 format.
#[derive(Debug)]
pub enum EpochMigrate {
    /// The pack is already v2 (or newer); nothing was done.
    AlreadyCurrent,
    /// The pack is a migratable legacy format but this was a dry run (`apply == false`); no write
    /// happened. The string describes what a migration would do.
    WouldMigrate(String),
    /// The pack was migrated to v2 (data log rewritten header-first, indexes rebuilt, re-sealed).
    /// The string describes what was done.
    Migrated(String),
    /// The legacy pack's data log is damaged and cannot be migrated without losing committed data;
    /// nothing was written and the epoch must be re-synced from peers. The string is the
    /// operator-facing reason.
    Corrupt(String),
}

/// A read-side heal of a sealed past epoch, built beside the live files and not yet visible. See
/// [`ConsensusPack::build_static_heal`] and [`ConsensusPack::install_static_heal`].
///
/// Owns its staging directory: a heal dropped without being installed (abandoned because the epoch
/// went live, or dropped on any other path) removes it, so a built copy is never left on disk.
#[derive(Debug)]
pub(crate) struct StaticHeal {
    /// `None` once [`ConsensusPack::install_static_heal`] has taken it.
    kind: Option<StaticHealKind>,
    /// Identity of the epoch's data log when the heal was built, so an install can tell the epoch
    /// was replaced (or appended to and re-sealed) in the meantime.
    data_identity: FileIdentity,
}

impl Drop for StaticHeal {
    fn drop(&mut self) {
        if let Some(StaticHealKind::Indexes(dir) | StaticHealKind::Migration(dir)) =
            self.kind.take()
        {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[derive(Debug)]
enum StaticHealKind {
    /// Derived indexes rebuilt from a cleanly sealed v2 data log, staged in this directory inside
    /// the epoch dir.
    Indexes(PathBuf),
    /// A v2 copy of a legacy (pre-v2) pack, staged in this `epoch-N.heal.migrating` directory.
    Migration(PathBuf),
}

/// A stable identity for a data log at one moment: (device, inode, length), so a replaced
/// file (a new inode renamed into place) and a file appended to and re-sealed since (same inode,
/// new length) are both told apart from the one a heal was built from.
type FileIdentity = (u64, u64, u64);

/// The [`FileIdentity`] of the file at `path`.
fn file_identity(path: &Path) -> Result<FileIdentity, PackError> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = std::fs::metadata(path)?;
    Ok((meta.dev(), meta.ino(), meta.len()))
}

/// Internal outcome of a migration copy/build step: either the source pack is damaged (reported to
/// the operator, nothing changed) or an I/O/build error occurred (propagated).
enum MigrateAbort {
    /// The source pack is damaged; nothing on disk was changed.
    Corrupt(String),
    /// An I/O or build error unrelated to the source's integrity.
    Fatal(PackError),
}

impl MigrateAbort {
    /// Collapse into a [`PackError`]: a damaged source is `CorruptPack` carrying the reason.
    fn into_pack_error(self) -> PackError {
        match self {
            Self::Corrupt(why) => PackError::CorruptPack(why),
            Self::Fatal(e) => e,
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
    ///
    /// The pack carries the committee from the on-disk meta, so after a mid-epoch restart of an
    /// imported epoch its fields outside the BLS key set are the serving peer's copy, even if an
    /// earlier [`Self::open_append`] ran with the chain-derived committee. Today they reach only
    /// telemetry; `verify_epoch_meta` is where any field becomes authenticated.
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

    /// Build, off to the side and without touching anything live, the read-side heal a sealed past
    /// epoch needs before [`Self::open_static`] can serve it, or `None` if it opens fine as is.
    ///
    /// - A pre-v2 (legacy) pack was indexed under the old digest-key placement, so its by-digest
    ///   lookups would silently miss present records: it is migrated to v2 in
    ///   `epoch-N.heal-{n}.migrating` (a staging dir of its own, so a writable open migrating the
    ///   same epoch in `epoch-N.migrating` never clobbers it). Migration copies the committed
    ///   outputs and refuses a log damaged below the acked frontier; it never truncates committed
    ///   data.
    /// - A cleanly sealed v2 pack whose derived index will not open, or disagrees with the log,
    ///   gets fresh indexes rebuilt from its WAL into a side directory inside the epoch dir. The
    ///   data log is only ever opened read-only: a crash mid-rebuild cannot leave it unsealed.
    /// - Anything else (no data log, or a torn/unclean v2 log) is refused with the terminal corrupt
    ///   error: a damaged past-epoch data log needs `db repair`, never a read-side heal
    ///   (INV1/INV4).
    ///
    /// Blocking and potentially long (a full WAL replay or copy): run it on a blocking thread. It
    /// only writes to its private side directory, so it needs no lock beyond keeping two builds of
    /// the same epoch apart; [`Self::install_static_heal`] makes the result visible.
    pub(crate) fn build_static_heal(
        path: &Path,
        epoch: Epoch,
    ) -> Result<Option<StaticHeal>, PackError> {
        let base_dir = path.join(format!("epoch-{epoch}"));
        let data_file = base_dir.join(Inner::DATA_NAME);
        let data_identity = file_identity(&data_file)?;
        // An environmental failure to open the data log (descriptor or memory exhaustion,
        // permissions) says nothing about the pack: surface it as I/O, which the heal back-off
        // does not remember, instead of an at-rest-corruption verdict replayed to every reader.
        let sealed_version = match Pack::<PackRecord>::open(
            &data_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        ) {
            Ok(data) => Some((data.version(), data.opened_unclean())),
            Err(DataFileOpen(LoadHeaderError::IO(e))) if io_error_is_environmental(&e) => {
                return Err(PackError::IO(Arc::new(e)));
            }
            Err(_) => None,
        };
        let kind = match sealed_version {
            Some((version, _)) if version < SENTINEL_MIN_VERSION => {
                warn!(
                    target: "consensus::pack",
                    epoch,
                    dir = %base_dir.display(),
                    "pre-v2 (legacy) epoch pack; migrating it to v2, which rebuilds its indexes"
                );
                let staging =
                    Inner::heal_staging_name(&format!("epoch-{epoch}.heal"), ".migrating");
                let (migrate_dir, _) = Inner::build_migration(path, epoch, &staging)
                    .map_err(MigrateAbort::into_pack_error)?;
                StaticHealKind::Migration(migrate_dir)
            }
            Some((_, false)) => {
                if Inner::open_static(path, epoch).is_ok() {
                    return Ok(None);
                }
                warn!(
                    target: "consensus::pack",
                    epoch,
                    dir = %base_dir.display(),
                    "sealed epoch has a clean data log but an unreadable or inconsistent index; \
                     rebuilding its indexes from the WAL"
                );
                StaticHealKind::Indexes(Inner::build_static_indexes(&base_dir, &data_file, epoch)?)
            }
            _ => return Err(Inner::corrupt_pack(&base_dir)),
        };
        Ok(Some(StaticHeal { kind: Some(kind), data_identity }))
    }

    /// Make a built [`StaticHeal`] visible: a few renames plus a directory fsync. The caller must
    /// hold `ConsensusChain::pack_install` and must not be installing over the live epoch.
    ///
    /// If the epoch's data log was replaced after the heal was built (an import or migration
    /// installed a new `epoch-N`), the build describes files that are gone: it is discarded (its
    /// staging removed as it drops) and nothing changes. The caller's next open sees whatever is
    /// there now.
    pub(crate) fn install_static_heal(
        path: &Path,
        epoch: Epoch,
        mut heal: StaticHeal,
    ) -> Result<(), PackError> {
        let base_dir = path.join(format!("epoch-{epoch}"));
        if file_identity(&base_dir.join(Inner::DATA_NAME)).ok() != Some(heal.data_identity) {
            return Ok(());
        }
        // Taken, so the install owns the staging from here (its own failure handling applies).
        match heal.kind.take() {
            Some(StaticHealKind::Indexes(side)) => Inner::install_static_indexes(&base_dir, &side),
            Some(StaticHealKind::Migration(migrate_dir)) => {
                Inner::install_migrated_dir(path, epoch, &migrate_dir)
            }
            None => Ok(()),
        }
    }

    /// Remove any read-side heal staging left inside `epoch_dir` by a crash mid-rebuild or
    /// mid-install. Only derived index copies live there: the data log is never staged.
    pub(crate) fn remove_stale_heal_dirs(epoch_dir: &Path) {
        let Ok(entries) = std::fs::read_dir(epoch_dir) else { return };
        for entry in entries.flatten() {
            if entry.file_name().to_str().is_some_and(|name| name.starts_with(Inner::REINDEX_DIR)) {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    /// Read-only check: does epoch `epoch`'s on-disk consensus pack predate the v2 (sentinel-era)
    /// format (see [`Self::is_legacy`])? `false` if the pack is missing/unopenable.
    #[cfg(test)]
    pub(crate) fn epoch_is_legacy<P: AsRef<Path>>(path: P, epoch: Epoch) -> bool {
        let data_file = path.as_ref().join(format!("epoch-{epoch}")).join(Inner::DATA_NAME);
        matches!(pack_unsealed_version(&data_file, epoch), Some((v, _)) if v < SENTINEL_MIN_VERSION)
    }

    /// Enumerate the epoch numbers that have an `epoch-{N}` directory under `epochs_dir`, sorted
    /// ascending. The highest is the current/live epoch (the one a running node holds open for
    /// append).
    pub fn epoch_dirs(epochs_dir: &Path) -> io::Result<Vec<Epoch>> {
        let mut epochs = Vec::new();
        for entry in std::fs::read_dir(epochs_dir)? {
            let entry = entry?;
            // `path().is_dir()` follows symlinks, so a symlinked `epoch-N/` is enumerated (the node
            // opens epochs by path and follows them); the destructive prune paths deliberately keep
            // the non-following `file_type()` check instead.
            if !entry.path().is_dir() {
                continue;
            }
            if let Some(n) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("epoch-"))
                .and_then(|num| num.parse::<Epoch>().ok())
            {
                epochs.push(n);
            }
        }
        epochs.sort_unstable();
        Ok(epochs)
    }

    /// Assess (and, when `apply`, repair) one epoch's consensus pack **at rest**.
    ///
    /// MUST run with the node stopped: with `apply` it opens the pack for append and rewrites it,
    /// which would corrupt a live node's mapping. A pack is healthy (and left untouched — a
    /// read-only open never writes) only when [`Self::open_static`] opens it cleanly AND full
    /// [`validate_pack_file`](crate::pack_validate::validate_pack_file) passes: `open_static` alone
    /// only checks the seal, cross-file lengths, the final position entry, and the FIRST
    /// digest-index bucket's CRC, so full validation (which walks the whole data stream and
    /// every bucket) is also required to catch a corrupt non-first bucket. A pack `open_static`
    /// rejects — or that validation flags — is damaged; the data file is classified with
    /// [`classify_physical_corruption`](crate::pack_validate::classify_physical_corruption) to
    /// decide whether a truncate-and-rebuild can recover it — a torn trailing record, or a
    /// physically-sound log whose sidecar indexes are missing/corrupt — or whether the damage
    /// is a lost epoch-meta / mid-log corruption that only a re-sync can fix.
    ///
    /// When repairable and `apply`, [`Self::open_append_exists`] truncates any torn tail and
    /// rebuilds every index from the data log, and the clean-close drop re-seals the pack; the
    /// result is then re-checked with `open_static` to confirm the pack is consistent.
    pub async fn repair_epoch(
        epochs_dir: &Path,
        epoch: Epoch,
        apply: bool,
    ) -> Result<EpochRepair, PackError> {
        let epoch_dir = epochs_dir.join(format!("epoch-{epoch}"));
        let data_file = epoch_dir.join(Inner::DATA_NAME);
        // A pre-v2 (v0/v1) pack predates the clean-close sentinel, so the seal-based classifier
        // below would mis-read a damaged legacy tail as a truncatable torn tail and drop
        // committed data. Never truncate a legacy pack: migrate it up to v2 (which rebuilds
        // indexes and re-seals from the data log) or, if its log is genuinely damaged below
        // the acked frontier, report it unrepairable. Peek the version read-only; a
        // header/open failure falls through to the normal classifier path (a corrupt
        // 28-byte header is handled there).
        let legacy = matches!(
            Pack::<PackRecord>::open(&data_file, epoch as u64, true, PackCompression::ZStd, PACK_VERSION),
            Ok(p) if p.version() < SENTINEL_MIN_VERSION
        );
        if legacy {
            match Inner::migrate_pack(epochs_dir, epoch, apply)? {
                EpochMigrate::AlreadyCurrent => return Ok(EpochRepair::Healthy),
                EpochMigrate::WouldMigrate(what) => return Ok(EpochRepair::WouldRepair(what)),
                EpochMigrate::Corrupt(why) => return Ok(EpochRepair::Unrepairable(why)),
                EpochMigrate::Migrated(what) => {
                    // Confirm the migrated pack now opens read-only cleanly AND fully validates.
                    Self::open_static(epochs_dir, epoch)?.close().await;
                    let report = crate::pack_validate::validate_pack_file(&data_file, epoch, None)?;
                    if report.verdict != crate::pack_validate::Verdict::Valid {
                        return Ok(EpochRepair::Unrepairable(format!(
                            "epoch {epoch}: migrated to v2 but validation still reports damage; the \
                             data itself is likely corrupt — re-sync the epoch from peers.\n{report}"
                        )));
                    }
                    return Ok(EpochRepair::Repaired(what));
                }
            }
        }
        // Healthy requires BOTH a clean read-only open AND full validation. `open_static` proves
        // the seal, cross-file lengths, final position entry, and the FIRST digest-index
        // bucket's CRC — but it does NOT scan the other buckets or the data stream, so on
        // its own it would call a corrupt non-first index bucket "healthy".
        // `validate_pack_file` (the `db validate` engine) walks the whole data stream and
        // every bucket CRC; requiring both closes that gap.
        let opens_clean = match Self::open_static(epochs_dir, epoch) {
            Ok(pack) => {
                pack.close().await;
                true
            }
            // An all-zero `data` file (a first write that sized the file but crashed before the
            // header was durable) is unwritten, not repairable: there is nothing to rebuild.
            // Report it with an actionable message rather than letting the classifier surface a
            // bare open error.
            Err(e) if e.is_unwritten_data_file() => {
                return Ok(EpochRepair::Unrepairable(format!(
                    "epoch {epoch}: the data file is all zeros (an interrupted first write); \
                     remove `epoch-{epoch}/` -- it is recreated on the next epoch transition."
                )));
            }
            Err(_) => false,
        };
        let validation = crate::pack_validate::validate_pack_file(&data_file, epoch, None).ok();
        let validates_clean = matches!(
            &validation,
            Some(report) if report.verdict == crate::pack_validate::Verdict::Valid
        );
        if opens_clean && validates_clean {
            return Ok(EpochRepair::Healthy);
        }
        let corruption = crate::pack_validate::classify_physical_corruption(&data_file, epoch)?;
        let plan = match &corruption {
            // The data log is physically sound; open_static failed on the indexes / seal / a length
            // disagreement — all of which recover_pack + the index rebuild fix.
            None => {
                // ...UNLESS validation flagged a defect in the data log itself (chain break, bad
                // number, missing/extra/unsorted batches, empty sub-dag, epoch-meta mismatch): a
                // rebuild-from-log cannot fix that, so report it Unrepairable up front -- on BOTH
                // the dry run and the apply -- rather than letting the dry run say
                // WouldRepair and the apply wipe+rebuild the indexes only to end
                // Unrepairable (dry run must predict apply).
                if validation.as_ref().is_some_and(|r| r.has_data_logical_issue()) {
                    // ...except an unclean pack whose only "defect" is an incomplete trailing
                    // output ending on a record boundary: the unacked in-flight write recovery
                    // truncates (INV1), not corruption.
                    match crate::pack_validate::incomplete_trailing_output(&data_file, epoch) {
                        Some(end) => format!(
                            "truncate the incomplete trailing output past offset {end} and rebuild \
                             indexes"
                        ),
                        None => {
                            return Ok(EpochRepair::Unrepairable(format!(
                                "epoch {epoch}: the data log has a logical defect a rebuild cannot \
                                 fix (a chain break, non-sequential number, missing/extra/unsorted \
                                 batches, an empty sub-dag, or an epoch-meta mismatch); the indexes \
                                 are not the problem. Re-sync the epoch from peers.\n{}",
                                validation.as_ref().map(ToString::to_string).unwrap_or_default()
                            )));
                        }
                    }
                } else {
                    "rebuild indexes and re-seal".to_string()
                }
            }
            Some(c) => match &c.kind {
                CorruptionKind::TornTrailingTail => {
                    "truncate the torn trailing record and rebuild indexes".to_string()
                }
                CorruptionKind::TornMetaEmpty => {
                    return Ok(EpochRepair::Unrepairable(format!(
                        "epoch {epoch}: the epoch-meta record is torn with no outputs behind it; the \
                         committee cannot be reconstructed locally. Remove `epoch-{epoch}/` and \
                         re-sync the epoch from peers."
                    )));
                }
                CorruptionKind::CorruptMetaWithData => {
                    return Ok(EpochRepair::Unrepairable(format!(
                        "epoch {epoch}: the epoch-meta is unreadable but complete outputs sit behind \
                         it (offset {}); those outputs are unreachable and truncation would lose \
                         them. Re-sync the epoch from peers.",
                        c.offset
                    )));
                }
                CorruptionKind::MidLogCorruption => {
                    return Ok(EpochRepair::Unrepairable(format!(
                        "epoch {epoch}: mid-log corruption at offset {} with valid records past it. \
                         If this data was durably committed it cannot be recovered by truncation — \
                         re-sync the epoch from peers. (Classification is conservative: for the \
                         current/most-recent epoch an unacked partial write can look the same, and a \
                         normal node restart runs full recovery, which may heal it.)",
                        c.offset
                    )));
                }
                CorruptionKind::CorruptSealedRecord => {
                    return Ok(EpochRepair::Unrepairable(format!(
                        "epoch {epoch}: a committed record failed its CRC in a cleanly-sealed pack \
                         at offset {}; the clean-close sentinel proves the log was complete, so this \
                         is at-rest corruption (bit rot), not a truncatable tail. Re-sync the epoch \
                         from peers.",
                        c.offset
                    )));
                }
            },
        };

        // Prove the repair can succeed BEFORE changing anything, with the same read-only checks the
        // apply's `recover_pack` makes in its pass 1 (the WAL replays, and no acked output lies
        // past where it stops: the tail commit marker and the position-index-attested
        // outputs). Two reasons. When `open_static` opened clean, `recover_pack` would
        // early-return on `files_consistent` and skip its own validation, so the index wipe
        // below must be proven here. And for every plan, the dry run must report the
        // verdict the apply would reach, not `WouldRepair` for a pack whose recovery then
        // refuses. A structurally unrebuildable log (a v0 batches-first pack, or a v1/v2
        // malformation the physical classifier misses) is reported `Unrepairable` with
        // nothing changed. Everything here reads only the data log and (read-only) the
        // position index, never the possibly-corrupt digest indexes.
        if let Err(e) = Inner::check_recoverable(&epoch_dir, &data_file, epoch) {
            return Ok(EpochRepair::Unrepairable(format!(
                "epoch {epoch}: the data log cannot be recovered without losing committed data \
                 ({e}); nothing was changed. Re-sync the epoch from peers."
            )));
        }

        if !apply {
            return Ok(EpochRepair::WouldRepair(plan));
        }

        // Force a rebuild when `open_static` opened clean: a length-consistent corrupt digest
        // bucket passes `files_consistent`, so the append open's `recover_pack` would
        // early-return and leave it untouched. Remove the derived digest indexes so the open must
        // rebuild them from the data-log WAL (`recover_pack` then wipes and rebuilds every index
        // anyway). The WAL was proven replayable above, so this wipe is always followed by a
        // successful rebuild. After the `!apply` return above, so a dry run writes nothing. Every
        // other repairable case has `open_static` already failing, so `recover_pack` runs on its
        // own and the indexes are left in place.
        if opens_clean {
            for name in [Inner::CONSENSUS_HASH_NAME, Inner::BATCH_HASH_NAME] {
                match std::fs::remove_dir_all(epoch_dir.join(name)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }

        // Apply: the writable open runs recover_pack (truncate torn tail) + open_indexes_for_append
        // (rebuild indexes); persist + drop re-seals. If recover_pack finds mid-log corruption the
        // best-effort classifier missed, this surfaces as an error -> Unrepairable.
        match Self::open_append_exists(epochs_dir, epoch) {
            Ok(pack) => {
                // Async-close (sole handle) so the background-thread join does not block a worker
                // on either the success or the persist-error path (never `?`-drop the sole handle).
                if let Err(e) = pack.persist().await {
                    pack.close().await;
                    return Err(e);
                }
                pack.close().await;
            }
            Err(e) => {
                return Ok(EpochRepair::Unrepairable(format!(
                    "epoch {epoch}: repair aborted, the data is damaged beyond truncation ({e}); \
                     re-sync the epoch from peers."
                )));
            }
        }
        // Confirm the repaired pack now opens read-only cleanly AND fully validates (`open_static`
        // alone would repeat the first-bucket blind spot). Async-close the sole handle so the
        // background-thread join does not block a worker (matching the healthy check above).
        Self::open_static(epochs_dir, epoch)?.close().await;
        let report = crate::pack_validate::validate_pack_file(&data_file, epoch, None)?;
        if report.verdict != crate::pack_validate::Verdict::Valid {
            return Ok(EpochRepair::Unrepairable(format!(
                "epoch {epoch}: rebuilt the indexes but validation still reports damage; the data \
                 itself is likely corrupt — re-sync the epoch from peers.\n{report}"
            )));
        }
        Ok(EpochRepair::Repaired(plan))
    }

    /// Migrate one epoch's pack from a pre-v2 (v0 batches-first / v1 header-first) format up to the
    /// current v2 format so it becomes a first-class, writable, sentinel-sealed pack.
    ///
    /// v2 is the only writable format: it carries the clean-close sentinel that lets recovery tell
    /// a truncatable unacked tail from at-rest corruption of committed data. A pre-v2 pack
    /// never carried a sentinel, so leaving it in place forces every recovery path to guess at
    /// "sealed" and risks truncating committed data. Migration rewrites the data log into a
    /// fresh v2 log (reordering a v0 batches-first log into the v1/v2 header-first layout),
    /// rebuilds the indexes from that log, and installs it atomically (rename-aside), so the
    /// original is untouched until the replacement is durably in place.
    ///
    /// The source is validated as it is read: any record that fails to decode, a size-prefix that
    /// desyncs the walk past a committed output, or a v0 output whose batch count disagrees with
    /// its header, yields [`EpochMigrate::Corrupt`] with nothing changed on disk (the operator
    /// re-syncs). An unacked crash tail of a header-first log is dropped, exactly as normal
    /// recovery would.
    ///
    /// Runs on the blocking pool: it is synchronous file work with no live actor, and can rewrite a
    /// large log, so it must not stall an async worker.
    pub async fn migrate_epoch(
        epochs_dir: &Path,
        epoch: Epoch,
        apply: bool,
    ) -> Result<EpochMigrate, PackError> {
        let epochs_dir = epochs_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Inner::migrate_pack(&epochs_dir, epoch, apply))
            .await
            .map_err(|e| PackError::PersistError(format!("migrate task join error: {e}")))?
    }

    /// Create a new set of epoch static files to write consensus output into, from a peer's
    /// `stream`, leaving at least [`IMPORT_MIN_FREE_BYTES`] free on the filesystem.
    pub async fn stream_import<P: Into<PathBuf>, R: AsyncRead + Unpin>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
    ) -> Result<ConsensusPack, PackError> {
        Self::stream_import_with_floor(
            path,
            stream,
            epoch,
            previous_epoch,
            final_consensus_number,
            timeout,
            IMPORT_MIN_FREE_BYTES,
        )
        .await
    }

    /// [`Self::stream_import`] with an explicit free-space floor: the import stops before the
    /// filesystem's free space drops below `min_free`. A restore from a local bundle the operator
    /// chose (`db load-state`) passes `0`, so a real shortage surfaces as the write error rather
    /// than a margin meant for peer-supplied bytes.
    pub async fn stream_import_with_floor<P: Into<PathBuf>, R: AsyncRead + Unpin>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
        min_free: u64,
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
            min_free,
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

    /// Does this pack predate the v2 (sentinel-era) format? A legacy pack is migrated to v2 before
    /// anything reads it: its digest indexes were written under the old key-placement scheme (so
    /// by-digest lookups would silently miss present records), and a v0 pack's batches-first
    /// outputs are not decoded at all outside the migration.
    pub fn is_legacy(&self) -> bool {
        self.version < SENTINEL_MIN_VERSION
    }

    /// Return the epoch for this pack file.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// True while the background actor thread is still serving requests. A `false` means the actor
    /// exited (it only dies via a panic — there is no `panic = "abort"`), after which every lookup
    /// wrapper collapses to `false`/`None`; the static-pack cache uses this to evict a dead handle
    /// so the next access re-opens the epoch fresh.
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Return the epoch-START committee this epoch's consensus output is decoded and verified
    /// against.
    ///
    /// Which copy depends on the door that opened the pack. After [`Self::open_append`] it is the
    /// chain-derived committee the caller passed, whether written as the new meta or matched
    /// against the on-disk one on the authenticated fields only. After
    /// [`Self::open_append_exists`], [`Self::open_static`] or an import it is the committee in
    /// the on-disk [`EpochMeta`].
    ///
    /// For a pack imported from a peer only the committee's BLS key set is authenticated (see
    /// [`verify_epoch_meta`]); its other fields (execution addresses, network keys, hosts, stake)
    /// are as the peer sent them, so nothing consensus-critical may rely on them.
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
    /// address) and compression, so the pack must be for the same epoch as the stream. Each record
    /// must arrive within `record_timeout`.
    pub async fn decode_output_stream<R: AsyncRead + Unpin>(
        &self,
        reader: R,
        expected_digest: ConsensusHeaderDigest,
        record_timeout: Duration,
    ) -> Result<ConsensusOutput, PackError> {
        bytes_to_verified_output(
            reader,
            self.compression,
            record_timeout,
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
        serve_output_bytes(bytes, self.version)
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
            if let Ok(found) = rx.await {
                return found;
            }
        }
        // A closed channel (dead actor) is not a real miss — surface it instead of a silent
        // `false`.
        error!(target: "consensus_pack", epoch = self.epoch(), "contains_consensus_header: pack actor unavailable (channel closed); reporting not-found");
        false
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(
        &self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ConsensusHeader(digest, tx)).await.is_ok() {
            if let Ok(header) = rx.await {
                return header;
            }
        }
        // A closed channel (dead actor) is not a real miss — surface it instead of a silent `None`.
        error!(target: "consensus_pack", epoch = self.epoch(), "consensus_header_by_digest: pack actor unavailable (channel closed); reporting not-found");
        None
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

    /// Durably commit the data file written since the last persist (an msync; size extensions are
    /// fsynced as they grow). Indexes are not synced: recovery rebuilds them from the data log.
    pub async fn persist(&self) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Persist(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }

    /// Flush buffered data to the page cache (visible to readers) without the fsync durability
    /// barrier that [`Self::persist`] provides.
    pub async fn flush_data(&self) -> Result<(), PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::FlushData(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)?
    }

    /// The logical data length (`end`) of the pack: the number of real record bytes, excluding the
    /// mmap capacity padding. A raw byte copy of the `data` file should bound its read to this so
    /// it captures exactly `[0, end)` (the immutable, append-only records) and never the
    /// trailing padding — regardless of the physical file size or any concurrent append.
    pub async fn data_file_len(&self) -> Result<u64, PackError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::DataFileLen(tx)).await;
        rx.await.map_err(|_| PackError::ReceiveFailed)
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

    /// The latest stored consensus number, defined for ANY pack with a valid meta: `start + N - 1`
    /// for N outputs, or `start - 1` (the previous epoch's final consensus number) for a meta-only
    /// pack. Used at startup to clamp a `LatestConsensus` hint a power loss left ahead of the
    /// recovered pack (a meta-only pack is the deterministic epoch-boundary case, where
    /// [`Self::latest_consensus_header`] returns `None`).
    pub async fn latest_consensus_number(&self) -> Result<u64, PackError> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::LatestConsensusNumber(tx)).await.is_ok() {
            rx.await.map_err(|_| PackError::SendFailed)
        } else {
            Err(PackError::SendFailed)
        }
    }

    /// True if the pack contains the batch for digest.
    pub async fn contains_batch(&self, digest: BlockHash) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::ContainsBatch(digest, tx)).await.is_ok() {
            if let Ok(found) = rx.await {
                return found;
            }
        }
        // A closed channel (dead actor) is not a real miss — surface it instead of a silent
        // `false`.
        error!(target: "consensus_pack", epoch = self.epoch(), "contains_batch: pack actor unavailable (channel closed); reporting not-found");
        false
    }

    /// Return the Batch for digest if found.
    pub async fn batch(&self, digest: BlockHash) -> Option<Batch> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::Batch(digest, tx)).await.is_ok() {
            if let Ok(batch) = rx.await {
                return batch;
            }
        }
        // A closed channel (dead actor) is not a real miss — surface it instead of a silent `None`.
        error!(target: "consensus_pack", epoch = self.epoch(), "batch: pack actor unavailable (channel closed); reporting not-found");
        None
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

    /// True when this is the only live handle to the pack, so [`Self::close`] will actually seal it
    /// (rather than no-op because another clone keeps the actor alive and defers the seal to a
    /// later `Drop`). Best-effort/racy — for diagnostics at an epoch handoff, not a
    /// synchronization point.
    pub fn is_sole_handle(&self) -> bool {
        Arc::strong_count(&self.handle) == 1
    }

    /// Take ownership and close async so we Drop does not get a chance to block any threads.
    /// Note, will only close if this is the last reference to this pack.
    /// Essentially this an async drop.
    pub async fn close(self) {
        if Arc::strong_count(&self.handle) == 1 {
            self.seal_now().await;
        }
    }

    /// Seal the pack now (clean-close: msync + truncate + sentinel via the actor's
    /// `AsyncShutdown`), REGARDLESS of how many clones remain. Idempotent and drop-safe: the
    /// join handle is taken under the lock, so only the first caller actually seals and a later
    /// call — or a surviving clone's `Drop` — finds it gone and no-ops (no double-seal, no
    /// panic).
    ///
    /// Used only at graceful shutdown, when a clone (e.g. a winding-down RPC connection) outlived
    /// the sole-owner drain: sealing under that clone is strictly better than leaving the pack
    /// unsealed and forcing a WAL recovery on the next start. The normal (sole-owner) path is
    /// [`Self::close`].
    ///
    /// ## Why this is memory-safe under a live clone
    /// The seal truncates the `data` file (dropping the mmap capacity padding) and stops the actor,
    /// but a surviving clone cannot dangle on the freed mapping, because a `ConsensusPack` clone is
    /// a CHANNEL-ONLY handle: it holds just `tx` + `handle` + `Copy` metadata, never a
    /// `MmapDataFile`. The mmaps live solely in the actor's `Inner`, and every read/write is
    /// mediated by a `PackMessage` on `tx` — so all mmap access happens on the one actor
    /// thread, never concurrently with (or after) the seal it runs itself. `MmapDataFile::drop`
    /// also unmaps BEFORE it truncates, and only the read-write current pack truncates
    /// (read-only static/staging packs early-return) — and only the padding past `end`, which
    /// no valid read touches. External byte-stream readers (`get_epoch_stream`, state export)
    /// use a cloned fd + `pread` bounded to `end`, never the shared mmap. Once the actor exits,
    /// a surviving clone's `tx.send`/`rx.await` return closed-channel errors — reads/writes
    /// fail cleanly (`None`/`Err`, logged), never touching freed memory.
    ///
    /// **Invariant this relies on:** never hand a clone direct mmap/file access to a pack's data —
    /// keep every read and write channel-mediated — or this guarantee breaks.
    pub(crate) async fn seal_now(&self) {
        let Some(_handle) = self.handle.lock().take() else {
            // Already sealed by a prior `seal_now`/`close` or a Drop; nothing to do.
            return;
        };
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::AsyncShutdown(tx)).await.is_ok() {
            // Async wait for the clean-close confirmation instead of a sync `join()`.
            let _ = rx.await;
        }
    }
}

/// File name of a pack's data log (the append-only WAL) within its `epoch-{N}` directory.
pub const DATA_NAME: &str = Inner::DATA_NAME;
/// Sidecar directory name of the consensus-header digest index (the `hash` hdx/odx).
pub const CONSENSUS_DIGEST_NAME: &str = Inner::CONSENSUS_HASH_NAME;
/// Sidecar directory name of the batch digest index (the `bhash` hdx/odx).
pub const BATCH_DIGEST_NAME: &str = Inner::BATCH_HASH_NAME;
/// Sidecar directory name of the position index (the `idx` pdx).
pub const POSITION_INDEX_NAME: &str = Inner::CONSENSUS_POS_NAME;

/// Whether any position-index-attested output starts after byte offset `from` and still decodes
/// from its recorded boundary.
///
/// The data-log walk in [`crate::pack_validate`] (and recovery's [`Inner::output_after_tear`])
/// advances by each record's claimed 4-byte size prefix, so a corrupted size prefix desyncs it and
/// it can miss a later intact output. The position index recorded that output's exact start, so
/// `fetch` frames it from a known-good offset a corrupted prefix cannot desync (the damaged
/// record's own `fetch` simply fails, so it is never miscounted as a survivor). A
/// missing/unreadable data pack or position index returns `false`, leaving the caller's walk-based
/// probe as the fallback. Read-only; opens its own handles.
pub(crate) fn attested_output_survives_past(data_path: &Path, epoch: Epoch, from: u64) -> bool {
    let Ok(mut data) = Pack::<PackRecord>::open(
        data_path,
        epoch as u64,
        true,
        PackCompression::ZStd,
        PACK_VERSION,
    ) else {
        return false;
    };
    let Some(epoch_dir) = data_path.parent() else {
        return false;
    };
    let offsets = attested_header_offsets(epoch_dir, data.header());
    Inner::attested_record_survives(&mut data, &offsets, from)
}

/// The output-header offsets the position index of the pack in `epoch_dir` records, read straight
/// from the index file (see [`PositionIndex::raw_entries`]) so the read-only checks attest what
/// [`Inner::recover_pack`] attests from its writable open. A read-only index open would refuse an
/// index a crash left unsealed (still capacity-padded, so not a whole number of entries), and the
/// checks would then attest nothing where recovery refuses. Empty when there is no readable index
/// for this data file.
fn attested_header_offsets(epoch_dir: &Path, data_header: &DataHeader) -> Vec<u64> {
    PositionIndex::<IndexPositions>::raw_entries(
        &epoch_dir.join(Inner::CONSENSUS_POS_NAME).join(Inner::CONSENSUS_POS_FILE),
        data_header,
    )
    .into_iter()
    .map(|position| position.consensus_header)
    .collect()
}

/// The offsets one position-index entry records for an output: `(consensus_header, output_start,
/// output_end)`.
pub(crate) type PositionEntry = (u64, u64, u64);

/// Read-only: decode every entry of the position index (`idx/index_pos.pdx`) beside `data_path`, in
/// order. An entry that fails its CRC/length check is an `Err` in place (the rest still decode).
/// `Ok(None)` when the pack has no position-index directory (a bare data file); `Err` when one
/// exists but will not open. Used by the offline validator to cross-check the index against the
/// data log, since opening a pack only checks the index's LAST entry.
pub(crate) fn read_position_entries(
    data_path: &Path,
    data_header: &DataHeader,
) -> Result<Option<Vec<Result<PositionEntry, FetchError>>>, PackError> {
    let Some(epoch_dir) = data_path.parent() else { return Ok(None) };
    if !epoch_dir.join(Inner::CONSENSUS_POS_NAME).is_dir() {
        return Ok(None);
    }
    let mut idx = Inner::open_pdx_file::<_, IndexPositions>(epoch_dir, data_header, true)?;
    Ok(Some(
        (0..idx.len() as u64)
            .map(|i| idx.load(i).map(|p| (p.consensus_header, p.output_start, p.output_end)))
            .collect(),
    ))
}

/// Read-only: would recovery of the pack whose data log is `data_path` refuse to truncate its
/// tail? `Err` is exactly what a writable open (or `db repair`) would refuse with; `Ok` means the
/// WAL replays and no acked output lies past where it stops (see `Inner::check_recoverable`).
pub(crate) fn check_recoverable(data_path: &Path, epoch: Epoch) -> Result<(), PackError> {
    let epoch_dir = data_path.parent().ok_or_else(|| {
        PackError::ReadError(format!("data path {} has no parent dir", data_path.display()))
    })?;
    Inner::check_recoverable(epoch_dir, data_path, epoch)
}

/// Read-only: why migrating the legacy (pre-v2) pack whose data log is `data_path` to v2 would
/// refuse, or `None` when it would succeed. See [`legacy_migration_dry_run`]. A legacy pack has no
/// clean-close sentinel, so this, not the v2 recovery check, is what decides whether its tail is
/// truncatable.
pub(crate) fn legacy_migration_refusal(data_path: &Path, epoch: Epoch) -> Option<String> {
    legacy_migration_dry_run(data_path, epoch).err()
}

/// Read-only: a dry run of migrating the legacy (pre-v2) pack whose data log is `data_path` to v2
/// (`db migrate` without `--force`). `Ok` with the number of outputs the migration would copy, or
/// why it refuses: an unacked torn tail is dropped by the migration, while damage below the acked
/// frontier is refused. This is the only check a legacy v0 pack gets, since it is only ever read
/// by its migration.
pub fn legacy_migration_dry_run(data_path: &Path, epoch: Epoch) -> Result<u64, String> {
    let base_dir = data_path
        .parent()
        .ok_or_else(|| format!("{} has no parent directory", data_path.display()))?;
    let src = Pack::<PackRecord>::open(
        data_path,
        epoch as u64,
        true,
        PackCompression::ZStd,
        PACK_VERSION,
    )
    .map_err(|e| PackError::from(e).to_string())?;
    Inner::migrate_copy(&src, None, src.version(), base_dir, data_path, epoch).map_err(
        |e| match e {
            MigrateAbort::Corrupt(why) => why,
            MigrateAbort::Fatal(e) => e.to_string(),
        },
    )
}

/// The byte offset just past the last COMPLETE consensus output in a pack's data log, computed by a
/// read-only WAL replay (no index is read or written). This is the safe point an unclean pack's
/// torn tail truncates back to; `db validate` bounds its logical prefix walk here so an in-flight
/// output's unwritten batches are not misreported as absent. Read-only; opens its own handle.
pub fn wal_consistent_end(data_path: &Path, epoch: Epoch) -> Result<u64, PackError> {
    let data = Pack::<PackRecord>::open(
        data_path,
        epoch as u64,
        true,
        PackCompression::ZStd,
        PACK_VERSION,
    )?;
    let base_dir = data_path.parent().ok_or_else(|| {
        PackError::ReadError(format!("data path {} has no parent dir", data_path.display()))
    })?;
    Inner::replay_wal(&data, base_dir, None)
}

/// Read-only peek of a pack's on-disk format `version` and whether it was NOT cleanly sealed
/// (`opened_unclean` — no clean-close sentinel). `None` if the data file cannot be opened. Used by
/// the `db validate` current-epoch warning to flag any pack a live writer may still be finishing
/// (the live current epoch, or a padded-unsealed previous epoch mid-handoff), whatever its epoch
/// number.
pub fn pack_unsealed_version(data_path: &Path, epoch: Epoch) -> Option<(u16, bool)> {
    let data = Pack::<PackRecord>::open(
        data_path,
        epoch as u64,
        true,
        PackCompression::ZStd,
        PACK_VERSION,
    )
    .ok()?;
    Some((data.version(), data.opened_unclean()))
}

/// Replace directory `live` (inside `parent`) with `staged` via rename-aside: the current `live`
/// is moved to `aside` and removed only after `staged` is renamed into place and `parent` is
/// fsync'd. If that rename fails the previous directory is restored (same inode), so a live pack
/// is never left on an absent path. A crash mid-swap leaves `aside` for
/// `ConsensusChain::recover_incomplete_installs` to restore or remove on the next start.
///
/// A leftover `aside` from an earlier interrupted install is deleted only when `live` exists (a
/// stale backup of a completed install). With `live` missing the aside is the last good copy, so
/// it is restored first rather than deleted.
pub(crate) fn install_dir_rename_aside(
    parent: &Path,
    live: &Path,
    aside: &Path,
    staged: &Path,
) -> io::Result<()> {
    if aside.exists() {
        if live.exists() {
            let _ = std::fs::remove_dir_all(aside);
        } else {
            std::fs::rename(aside, live)?;
        }
    }
    let had_old = live.exists();
    if had_old {
        std::fs::rename(live, aside)?;
    }
    let installed = std::fs::rename(staged, live);
    if installed.is_err() && had_old {
        if let Err(restore_err) = std::fs::rename(aside, live) {
            error!(
                target: "consensus::store",
                %restore_err,
                live = %live.display(),
                aside = %aside.display(),
                "install rename failed AND the restore rename failed; the previous copy is only at \
                 the aside path and will be rolled back on the next startup"
            );
        }
    }
    installed?;
    fsync_directory(parent)?;
    if had_old {
        // Best-effort: a crash before this leaves the aside for startup cleanup to remove.
        let _ = std::fs::remove_dir_all(aside);
    }
    Ok(())
}

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
    /// Test-only: when set, the next `save_consensus_output` appends and indexes the output fully,
    /// then returns an error before `Ok`, so the atomic-rollback path can be exercised at its
    /// worst case (data appended, indexes advanced).
    #[cfg(test)]
    fail_save_after_append: bool,
}

impl Inner {
    const DATA_NAME: &str = "data";
    const CONSENSUS_POS_NAME: &str = "idx";
    /// The position index file inside [`Self::CONSENSUS_POS_NAME`].
    const CONSENSUS_POS_FILE: &str = "index_pos.pdx";
    const CONSENSUS_HASH_NAME: &str = "hash";
    const BATCH_HASH_NAME: &str = "bhash";

    /// Determine if the pack and indexes appear to have been closed cleanly.
    ///
    /// The primary, definitive test is the clean-close sentinel: every backing file (the data log,
    /// the position index, and both digest indexes) must have been sealed by a clean shutdown
    /// (`!opened_unclean()`). A missing sentinel on any of them means that file was not cleanly
    /// closed (most likely padded/torn after a crash) and the pack must be recovered. This catches
    /// cases the length checks alone miss — e.g. a crash that left a file exactly at capacity,
    /// where physical == logical == the index markers yet the tail record may be torn.
    ///
    /// The sentinel is a v2-format feature. Pre-sentinel packs (v0/v1, below
    /// [`SENTINEL_MIN_VERSION`]) never carried one — the buffered backend wrote physical == logical
    /// with no trailing sentinel — so for them a missing sentinel is expected and must not force
    /// recovery. The gate below is therefore applied only to v2+ files; a legacy pack falls through
    /// to the length cross-checks alone, which is exactly the length-only test pre-mmap `main`
    /// used, so an existing datadir opens as it did before the sentinel existed. Real damage in
    /// a legacy pack still trips the length checks here (and, on the writable door, WAL
    /// replay).
    ///
    /// The length comparisons below remain as a secondary cross-file integrity check: even a sealed
    /// pack is only consistent if the data length agrees with what both digest indexes and the
    /// position index attest.
    fn files_consistent(
        data: &Pack<PackRecord>,
        consensus_pos_idx: &mut PositionIndex<IndexPositions>,
        consensus_digests: &HdxIndex,
        batch_digests: &HdxIndex,
    ) -> bool {
        // Primary gate: any *sentinel-era* (v2+) file that was not cleanly sealed forces recovery.
        // Pre-sentinel packs (v0/v1) never wrote a sentinel, so a missing one is not an unclean
        // signal for them — they rely on the length cross-checks below instead.
        if data.version() >= SENTINEL_MIN_VERSION
            && (data.opened_unclean()
                || consensus_pos_idx.opened_unclean()
                || consensus_digests.opened_unclean()
                || batch_digests.opened_unclean())
        {
            return false;
        }
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
            // No output is positioned. That is consistent only for a log that holds none: a
            // digest index with entries beside an empty position index means the position index
            // was lost (its directory removed and recreated empty), and every by-number read would
            // miss while the digest markers still vouch for the whole log.
            consensus_digests.is_empty()
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
    /// Header-first (v1/v2) format only. `open_static` rejects an inconsistent read-only pack
    /// rather than healing, so recovery only runs on the writable append opens. Those may open
    /// a legacy v1 pack (the in-flight epoch at upgrade) as well as current v2 packs; both
    /// replay identically. An inconsistent v0 (batches-first) pack cannot be replayed and is
    /// rejected up front with a re-sync message — see the guard below.
    fn recover_pack<P: AsRef<Path>>(
        data: &mut Pack<PackRecord>,
        base_dir: P,
        mut consensus_pos_idx: PositionIndex<IndexPositions>,
        mut consensus_digests: HdxIndex,
        mut batch_digests: HdxIndex,
    ) -> Result<(PositionIndex<IndexPositions>, HdxIndex, HdxIndex), PackError> {
        if Self::files_consistent(data, &mut consensus_pos_idx, &consensus_digests, &batch_digests)
        {
            // Already consistent: nothing to rebuild. Mark every handle consistent so a clean
            // `Drop` (re-)seals it. For a v2 pack reaching here all four are already
            // clean (no-op); for a consistent legacy v1 pack (its length cross-checks
            // pass without a sentinel) this preserves the previous always-seal-on-close
            // behaviour.
            data.mark_consistent();
            consensus_pos_idx.mark_consistent();
            consensus_digests.mark_consistent();
            batch_digests.mark_consistent();
            return Ok((consensus_pos_idx, consensus_digests, batch_digests));
        }
        let base_dir = base_dir.as_ref();
        // `replay_wal` below understands only the v1/v2 header-first layout. A *consistent* v0 pack
        // never reaches here — the length cross-checks in `files_consistent` pass for it without a
        // sentinel — so the only way to arrive holding a v0 pack is an inconsistent one: a v0 log
        // the mmap backend appended to and then crashed, leaving padding or a torn tail. Replaying
        // its batches-first records as v1 would misread them as mid-log corruption, so surface an
        // honest error instead. Unreachable in practice: the current (appendable) epoch after an
        // upgrade is v1, and every v0 pack on disk is a sealed static epoch opened read-only via
        // `open_static` (which never calls this).
        if data.version() == 0 {
            return Err(PackError::CorruptPack(format!(
                "epoch pack {} is a pre-mmap v0 (batches-first) log left inconsistent after an \
                 unclean shutdown; it cannot be rebuilt by replay and must be re-synced from peers. \
                 Do NOT delete the chain-data directories (`db`, `static_files`, `consensus-db`)",
                base_dir.display(),
            )));
        }
        // Recovery replays the whole data-file WAL, so it can take time proportional to pack size.
        // Log the start (and the completion below) so a slow recovery is observable rather than a
        // silent stall at startup.
        let recover_start = std::time::Instant::now();
        info!(
            target: "consensus_pack",
            dir = %base_dir.display(),
            "pack opened unclean or inconsistent; replaying data-file WAL to recover"
        );
        // Capture the position index's attested output-start offsets before any reset below (pass 1
        // reads no index, so they are still intact). The WAL walk in
        // `replay_wal`/`output_after_tear` advances by each record's claimed size, so a
        // corrupted size prefix desyncs it and it can stop early, missing a later intact
        // output; these recorded boundaries let the post-replay check re-frame such an
        // output from a known-good offset (see `attested_record_survives`). Empty when the
        // index was itself discarded (`open_index_for_append`) -> falls back to the walk.
        let attested_headers: Vec<u64> = (0..consensus_pos_idx.len() as u64)
            .filter_map(|i| consensus_pos_idx.load(i).ok().map(|p| p.consensus_header))
            .collect();
        // Pass 1 -- validate the data-log WAL ALONE (no index is read or written). A detected
        // corruption returns `CorruptPack` without mutating on-disk state, so a retry re-derives
        // the same verdict from the unchanged log. `replay_wal` returns the end of the last
        // complete output and rejects a tear that has a later *complete output* after it:
        // an output written past the tear can only exist if the earlier one was durably
        // committed first, so the damage is corruption, not the single unacked in-flight
        // tail. (Production persists after every output, so at most one output is ever
        // unacked at the physical tail -- see `persist`.)
        let consistent_end = Self::replay_wal(data, base_dir, None)?;

        // Close the one gap the probe cannot see from structure alone: at-rest corruption of the
        // LAST committed output with nothing decodable after it. `persist()` writes a best-effort
        // tail commit marker recording the durable acked end, index-free; because it is stamped
        // AFTER the data msync it can never sit ahead of durable data, so a replay that stops below
        // it means acked data was damaged. A missing/stale marker just falls back to the probe.
        if let Some(committed_end) = data.committed_end() {
            if consistent_end < committed_end {
                return Err(Self::corrupt_pack(base_dir));
            }
        }

        // A position-index-attested output surviving past the replay's stopping point means
        // committed data below the acked frontier was damaged -- e.g. a corrupted 4-byte
        // size prefix desynced the WAL walk so `replay_wal` stopped early and
        // `output_after_tear` could not re-sync to the later output. Re-framing from the
        // recorded boundary is immune to that desync, closing the size-prefix gap the walk
        // cannot (INV1/INV4: a hard `CorruptPack` below acked data, never a
        // silent truncate). No-op when the index was discarded (empty `attested_headers`).
        if Self::attested_record_survives(data, &attested_headers, consistent_end) {
            return Err(Self::corrupt_pack(base_dir));
        }

        // Validation passed: the data log is authoritative, so discard the (stale/damaged) indexes
        // and rebuild. The digest indexes are directories (index.hdx + index.odx), so remove the
        // whole directory.
        consensus_pos_idx.truncate_all()?;
        drop(consensus_digests);
        drop(batch_digests);
        std::fs::remove_dir_all(base_dir.join(Self::CONSENSUS_HASH_NAME))?;
        std::fs::remove_dir_all(base_dir.join(Self::BATCH_HASH_NAME))?;
        let (mut consensus_digests, mut batch_digests) =
            Self::open_digest_indexes(base_dir, data.header(), false)?;

        // Pass 2: replay again, writing every recovered position/digest into the fresh indexes.
        // The data log is unchanged between passes, so this returns the same `consistent_end`.
        let consistent_end = Self::replay_wal(
            data,
            base_dir,
            Some((&mut consensus_pos_idx, &mut consensus_digests, &mut batch_digests)),
        )?;

        // Drop any incomplete/torn tail so the log ends exactly at the last complete output.
        // `rewind_to` (not `truncate`) keeps the mmap capacity and opens no read-only-mmap SIGBUS
        // window -- the same primitive `rollback_output` uses to undo a partial append below.
        if consistent_end < data.file_len() {
            data.rewind_to(consistent_end);
        }
        // Reconcile the digest indexes' tracked data length with the (possibly truncated) log so
        // `files_consistent` holds on the next open even if no save follows this recovery.
        let len = data.file_len();
        consensus_digests.set_data_file_length(len);
        batch_digests.set_data_file_length(len);
        info!(
            target: "consensus_pack",
            dir = %base_dir.display(),
            records = consensus_pos_idx.len(),
            recovered_end = consistent_end,
            elapsed_ms = recover_start.elapsed().as_millis() as u64,
            "pack WAL recovery complete"
        );
        // Recovery rewound the log to its last complete output and rebuilt the indexes from it, so
        // all four handles are now self-consistent. Clear their unclean flags so the clean `Drop`
        // re-seals them and the next open skips this replay (rather than rebuilding on every
        // restart). A `write_failed` durability failure still independently blocks the seal, so
        // this can never seal a tail that did not reach disk.
        data.mark_consistent();
        consensus_pos_idx.mark_consistent();
        consensus_digests.mark_consistent();
        batch_digests.mark_consistent();
        Ok((consensus_pos_idx, consensus_digests, batch_digests))
    }

    /// Replay the data-log WAL once, returning the byte offset just past the last complete output
    /// (`consistent_end`) and rejecting mid-log corruption from the data alone (no index is read).
    ///
    /// A torn/incomplete output ends the consistent prefix. It is truncatable UNLESS a later,
    /// well-formed OUTPUT (`output_after_tear`) decodes past the tear: production persists after
    /// every output, so at most one output is ever unacked at the tail, and a *complete* output
    /// past the tear can only exist if the earlier one was durably committed first -- so its
    /// damage is at-rest corruption of committed data ([`PackError::CorruptPack`]), not the
    /// single unacked in-flight tail. (At-rest corruption of the last output with nothing after
    /// it is covered by the commit marker in [`Self::recover_pack`].)
    ///
    /// A *cleanly-sealed* log (`!opened_unclean()`) is complete by construction — the clean-close
    /// sentinel is written only after the tail is msync'd and truncated to `end` — so it can hold
    /// no torn tail: ANY tear during replay is at-rest corruption and a hard `CorruptPack`.
    ///
    /// When `sink` is `None` the log is only *validated* — no index is touched — so a detected
    /// corruption returns without mutating any on-disk state and a retry re-derives the same
    /// verdict from the unchanged log. When `sink` is `Some`, each recovered output's
    /// header/batch digests and position are written into the provided indexes (the rebuild
    /// pass).
    ///
    /// Uses `logical_position` (advanced only by whole record frames), never the physical
    /// `position`, so the returned offset can never land mid-record even after a torn read.
    fn replay_wal(
        data: &Pack<PackRecord>,
        base_dir: &Path,
        mut sink: Option<(&mut PositionIndex<IndexPositions>, &mut HdxIndex, &mut HdxIndex)>,
    ) -> Result<u64, PackError> {
        let mut iter = data.raw_iter().map_err(DataFileOpen)?;
        // A cleanly-sealed log (clean-close sentinel present) is complete by construction — the
        // seal is written only after the tail is msync'd and truncated to `end`. So it can
        // hold no torn tail: ANY replay tear is at-rest corruption (bit rot), a hard
        // `CorruptPack`. Only an *unclean* (crash-interrupted) log can have a truncatable
        // tail, decided below.
        let sealed = !data.opened_unclean();
        // 0-based local index of the output within this pack (mirrors `save_consensus_output`).
        let mut idx: u64 = 0;
        // Byte offset just past the last fully-recovered record (the EpochMeta or a complete
        // output). Anything after it is an incomplete/torn tail and is truncated away by the
        // caller.
        let mut consistent_end = iter.logical_position();
        // Only the very first record may be an EpochMeta; a second one mid-log is
        // append-order-impossible (see the EpochMeta arm below).
        let mut first_record = true;
        // Throttled progress so a long unclean-restart replay (this runs synchronously in
        // `ConsensusChain::new`, and can be seconds for a multi-GB epoch) is observable between the
        // start/finish lines `recover_pack` logs — at most one line every few seconds.
        let mut last_progress = std::time::Instant::now();

        loop {
            let header_pos = iter.logical_position();
            let is_first = first_record;
            first_record = false;
            match iter.next() {
                // Clean EOF on an output boundary: every complete output has been replayed.
                None => break,
                // The leading EpochMeta carries no index data (epoch_meta is already loaded and the
                // pos index is 0-based); skip it, but keep it in the consistent prefix. A
                // *non-leading* EpochMeta is structurally impossible in append order (each pack is
                // written with exactly one, first) -- treat it as corruption, matching
                // `validate_pack_file`, rather than silently folding it into the consistent prefix
                // (which would leave the on-disk log validating as damaged after any repair).
                Some(Ok(PackRecord::EpochMeta(_))) => {
                    if !is_first {
                        return Err(Self::corrupt_pack(base_dir));
                    }
                    consistent_end = iter.logical_position();
                    continue;
                }
                Some(Ok(PackRecord::Consensus(consensus_header))) => {
                    if let Some((_, consensus_digests, _)) = sink.as_mut() {
                        consensus_digests
                            .save(consensus_header.digest().into(), header_pos)
                            .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
                    }
                    // The header's sub-dag names exactly the batch records this output owns.
                    let expected = Self::expected_batch_count(&consensus_header);
                    let mut torn = false;
                    for _ in 0..expected {
                        let batch_pos = iter.logical_position();
                        match iter.next() {
                            Some(Ok(PackRecord::Batch(batch))) => {
                                if let Some((_, _, batch_digests)) = sink.as_mut() {
                                    batch_digests.save(batch.digest(), batch_pos).map_err(|e| {
                                        PackError::IndexAppend(format!("batch {e}"))
                                    })?;
                                }
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
                        // Incomplete output ends the consistent prefix. In a sealed log any tear is
                        // corruption. In an unclean log, dropping it is safe unless a later
                        // well-formed OUTPUT decodes past the tear -- that output was written after
                        // this one was durably committed, so the damage is corruption of committed
                        // data, not the single unacked in-flight tail.
                        if sealed || Self::output_after_tear(&mut iter) {
                            return Err(Self::corrupt_pack(base_dir));
                        }
                        break; // consistent_end still marks the end of the last complete output
                    }
                    let output_end = iter.logical_position();
                    if let Some((consensus_pos_idx, _, _)) = sink.as_mut() {
                        consensus_pos_idx
                            .save(idx, IndexPositions::new(header_pos, header_pos, output_end))
                            .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
                    }
                    idx += 1;
                    consistent_end = output_end;
                    if last_progress.elapsed() >= std::time::Duration::from_secs(5) {
                        info!(
                            target: "consensus_pack",
                            dir = %base_dir.display(),
                            outputs = idx,
                            recovered_bytes = consistent_end,
                            "pack WAL recovery in progress"
                        );
                        last_progress = std::time::Instant::now();
                    }
                }
                // A torn record where the next output's header would start. The last complete
                // output is already finalized; this ends the consistent prefix. In a sealed log any
                // tear is corruption. In an unclean log it is fatal only when a later well-formed
                // OUTPUT still decodes past the tear (corruption of committed data); otherwise it
                // is the unacked in-flight tail, safe to drop.
                Some(Err(_)) => {
                    if sealed || Self::output_after_tear(&mut iter) {
                        return Err(Self::corrupt_pack(base_dir));
                    }
                    break;
                }
                // v1 is header-first, so a decodable batch where a header is expected is
                // append-order-impossible -- genuine corruption, not a tail. (A stray second
                // EpochMeta is rejected above in the EpochMeta arm.)
                Some(Ok(_)) => return Err(Self::corrupt_pack(base_dir)),
            }
        }
        Ok(consistent_end)
    }

    /// Read-only form of [`Self::recover_pack`]'s pass-1 checks for the pack in `epoch_dir`: the
    /// data-log WAL replays, and no acked output survives past where it stops (the tail commit
    /// marker, or a position-index-attested output that still decodes). `Err` is exactly what
    /// recovery would refuse with. Nothing is written.
    fn check_recoverable(
        epoch_dir: &Path,
        data_file: &Path,
        epoch: Epoch,
    ) -> Result<(), PackError> {
        let mut data = Pack::<PackRecord>::open(
            data_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let attested_headers = attested_header_offsets(epoch_dir, data.header());
        let consistent_end = Self::replay_wal(&data, epoch_dir, None)?;
        if data.committed_end().is_some_and(|committed| consistent_end < committed)
            || Self::attested_record_survives(&mut data, &attested_headers, consistent_end)
        {
            return Err(Self::corrupt_pack(epoch_dir));
        }
        Ok(())
    }

    /// Number of batch records the output for `header` owns — the dedup of its sub-dag's payload
    /// digests, matching what `save_consensus_batches` writes via `collect_batches`. Zero when the
    /// output references no batches.
    fn expected_batch_count(header: &ConsensusHeader) -> usize {
        declared_batch_digests(header).len()
    }

    /// A [`PackError::CorruptPack`] carrying the pack location and operator remediation, for when
    /// recovery finds durably-committed data damaged (a tear below the acked watermark, or a
    /// structurally impossible record). Unlike an unacked torn tail this cannot be healed by
    /// truncation, so the message points the operator at `db validate` and warns off the chain-data
    /// directories.
    fn corrupt_pack(base_dir: &Path) -> PackError {
        PackError::CorruptPack(format!(
            "epoch pack {} is corrupt. Inspect it with `telcoin-network db validate {}`: if it \
             reports a truncatable torn tail or an index problem, `telcoin-network db repair` (node \
             stopped) can repair it; if it reports durably-committed data damaged, that cannot be \
             repaired by truncation and the epoch must be re-synced from peers. Do NOT delete the \
             chain-data directories (`db`, `static_files`, `consensus-db`)",
            base_dir.display(),
            base_dir.display(),
        ))
    }

    /// A [`PackError::CorruptPack`] for a *read-only* open (a sealed past epoch) whose derived
    /// index is damaged at rest -- a corrupt header, a torn/misaligned tail, or a
    /// geometry/uid/hasher mismatch. A read-only door cannot rebuild an index in place, so this
    /// is terminal; but unlike a torn data log the `data` file is the authoritative source and
    /// is likely intact, so the message steers the operator to `db validate` (to confirm the
    /// data) rather than implying data loss, and warns off deleting the data / chain-data
    /// directories. Distinct from [`Self::corrupt_pack`] so the terse `LoadHeaderError` (e.g.
    /// "invalid index bucket geometry") never reaches the operator.
    fn corrupt_static_index(base_dir: &Path, epoch: Epoch, cause: &PackError) -> PackError {
        PackError::CorruptPack(format!(
            "epoch {epoch} pack {}: a derived index is damaged and a read-only open cannot rebuild \
             it ({cause}). The `data` log is the source of truth, so run `telcoin-network db repair \
             --epoch {epoch}` (with the node stopped) to rebuild the index from the log; run \
             `telcoin-network db validate {}` first to confirm the data is intact. Do NOT delete the \
             `data` file or the chain-data directories (`db`, `static_files`, `consensus-db`)",
            base_dir.display(),
            base_dir.display(),
        ))
    }

    /// Does any position-index-attested output start after byte offset `from` and still decode as a
    /// `Consensus` record? [`Self::output_after_tear`] walks by each record's claimed 4-byte size
    /// prefix, so a corrupted prefix desyncs it and it can miss a later intact output; the position
    /// index recorded that output's exact start, so `fetch` frames it from a known-good boundary (a
    /// corrupted prefix just makes that `fetch` fail, so it is never miscounted). Empty `headers`
    /// (the index was itself discarded and is being rebuilt) makes this a no-op, leaving
    /// `output_after_tear` as the fallback.
    fn attested_record_survives(data: &mut Pack<PackRecord>, headers: &[u64], from: u64) -> bool {
        headers
            .iter()
            .filter(|&&pos| pos > from)
            .any(|&pos| matches!(data.fetch(pos), Ok(PackRecord::Consensus(_))))
    }

    /// After recovery hits a torn/incomplete output, decide whether the rest of the log is a clean
    /// unacked tail (safe to truncate) or corruption of committed data (an error). Because
    /// production persists after every output, at most one output is ever unacked at the tail,
    /// so its leftovers are only its own `Batch` records — never a new `Consensus` header. A
    /// decodable `Consensus` header past the tear therefore means a *later output* was written,
    /// which can only have happened after the earlier one was durably committed: the earlier
    /// damage is corruption.
    ///
    /// Returns `true` (corruption) iff a later output header decodes before EOF; stray batches, a
    /// stray meta, and CRC-failed frames are skipped. The `position` no-forward-progress guard
    /// (mirrors `pack_validate::probe_decodable_after`) stops a size-prefix-past-EOF from spinning.
    fn output_after_tear(iter: &mut crate::archive::pack::RawIter<PackRecord>) -> bool {
        // `logical_position` (bytes consumed to the last frame boundary) is the syscall-free
        // position — it advances by each frame's on-disk size, including a CRC-failed
        // zero-padding frame, so it gives the same monotonic forward-progress signal as the
        // physical position without an `lseek` per frame (a 128 MiB zero-padding walk on an
        // unclean open is otherwise ~5 s of lseeks).
        let mut last_pos = iter.logical_position();
        loop {
            match iter.next() {
                None => return false,
                // A later output began → an output was written past the tear → corruption.
                Some(Ok(PackRecord::Consensus(_))) => return true,
                // A stray batch/meta of the torn in-flight output: not a new output; keep scanning.
                Some(Ok(_)) => last_pos = iter.logical_position(),
                Some(Err(_)) => {
                    let pos = iter.logical_position();
                    if pos <= last_pos {
                        return false; // no forward progress (extent past EOF): nothing readable
                                      // after
                    }
                    last_pos = pos;
                }
            }
        }
    }

    /// Migrate an `epoch-{epoch}` pack under `epochs_dir` from a pre-v2 format to v2. See
    /// [`ConsensusPack::migrate_epoch`] for the operator-facing contract. Synchronous; no live
    /// actor.
    fn migrate_pack(
        epochs_dir: &Path,
        epoch: Epoch,
        apply: bool,
    ) -> Result<EpochMigrate, PackError> {
        let base_dir = epochs_dir.join(format!("epoch-{epoch}"));
        let data_file = base_dir.join(Self::DATA_NAME);
        // Read-only peek of the on-disk version. A pack already at (or past) the current format
        // needs no migration.
        let src = Pack::<PackRecord>::open(
            &data_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let version = src.version();
        if version >= PACK_VERSION {
            return Ok(EpochMigrate::AlreadyCurrent);
        }

        // Dry run: validate and count without writing anything.
        if !apply {
            return Ok(
                match Self::migrate_copy(&src, None, version, &base_dir, &data_file, epoch) {
                    Ok(n) => EpochMigrate::WouldMigrate(format!(
                        "v{version} -> v{PACK_VERSION}, {n} output(s)"
                    )),
                    Err(MigrateAbort::Corrupt(why)) => EpochMigrate::Corrupt(why),
                    Err(MigrateAbort::Fatal(e)) => return Err(e),
                },
            );
        }

        // Apply: build a fresh v2 pack in a sibling temp dir, then install it atomically. The
        // original `epoch-{epoch}` dir is untouched until the replacement is durably in place, so a
        // crash or an error leaves the legacy pack readable and re-migratable.
        drop(src);
        let staging = format!("epoch-{epoch}.migrating");
        let (migrate_dir, n) = match Self::build_migration(epochs_dir, epoch, &staging) {
            Ok(built) => built,
            Err(MigrateAbort::Corrupt(why)) => return Ok(EpochMigrate::Corrupt(why)),
            Err(MigrateAbort::Fatal(e)) => return Err(e),
        };
        Self::install_migrated_dir(epochs_dir, epoch, &migrate_dir)?;
        Ok(EpochMigrate::Migrated(format!("v{version} -> v{PACK_VERSION}, {n} output(s)")))
    }

    /// Build a v2 copy of the legacy pack `epoch-{epoch}` under `epochs_dir` in its `staging`
    /// dir there (named `*.migrating`, so startup sweeps a leftover), not yet installed, carrying
    /// over the epoch's per-epoch certificate pack. Returns the staging directory and the
    /// number of outputs copied. On any error the staging directory is removed and the legacy
    /// pack is untouched.
    fn build_migration(
        epochs_dir: &Path,
        epoch: Epoch,
        staging: &str,
    ) -> Result<(PathBuf, u64), MigrateAbort> {
        let base_dir = epochs_dir.join(format!("epoch-{epoch}"));
        let data_file = base_dir.join(Self::DATA_NAME);
        let src = Pack::<PackRecord>::open(
            &data_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )
        .map_err(|e| MigrateAbort::Fatal(e.into()))?;
        let version = src.version();
        let migrate_dir = epochs_dir.join(staging);
        let _ = std::fs::remove_dir_all(&migrate_dir);
        create_dir_synced(&migrate_dir).map_err(|e| MigrateAbort::Fatal(e.into()))?;
        let built =
            Self::build_migrated_pack(&src, version, &base_dir, &data_file, epoch, &migrate_dir)
                .and_then(|n| {
                    Self::copy_cert_pack(&base_dir, &migrate_dir)
                        .map_err(|e| MigrateAbort::Fatal(e.into()))?;
                    Ok(n)
                });
        match built {
            Ok(n) => Ok((migrate_dir, n)),
            Err(e) => {
                let _ = std::fs::remove_dir_all(&migrate_dir);
                Err(e)
            }
        }
    }

    /// Copy the epoch's per-epoch certificate pack (`cert_data` and its `cert_hash/` index) from
    /// `base_dir` into a migration staging dir, so installing the migrated `epoch-N` does not drop
    /// it. The certificate pack is a separate pack with its own format; it is carried over as is.
    /// Absent files are skipped (an imported or observer epoch has none).
    fn copy_cert_pack(base_dir: &Path, migrate_dir: &Path) -> io::Result<()> {
        // Each copy is synced before the install renames the migration into place and removes the
        // original: a directory fsync alone makes the new entries durable, not their contents.
        let copy_synced = |from: &Path, to: &Path| -> io::Result<()> {
            std::fs::copy(from, to)?;
            std::fs::File::open(to)?.sync_all()
        };
        let data = base_dir.join(crate::certificate_pack::DATA_NAME);
        if data.is_file() {
            copy_synced(&data, &migrate_dir.join(crate::certificate_pack::DATA_NAME))?;
        }
        let hash = base_dir.join(crate::certificate_pack::HASH_NAME);
        if hash.is_dir() {
            let dst = migrate_dir.join(crate::certificate_pack::HASH_NAME);
            std::fs::create_dir_all(&dst)?;
            for entry in std::fs::read_dir(&hash)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    copy_synced(&entry.path(), &dst.join(entry.file_name()))?;
                }
            }
            fsync_directory(&dst)?;
        }
        fsync_directory(migrate_dir)
    }

    /// If an existing `epoch-{epoch}` pack under `epochs_dir` is a pre-v2 format, migrate it up to
    /// v2 before it is opened for write — v2 is the only writable format, so writing v2
    /// records/sentinels onto a v0/v1-versioned header would produce an inconsistent
    /// mixed-format pack. A cheap read-only no-op for a pack already at v2 (the common case). A
    /// genuinely corrupt legacy log aborts with [`PackError::CorruptPack`] (re-sync) — the same
    /// failure class as an unreadable meta.
    fn migrate_legacy_if_needed(epochs_dir: &Path, epoch: Epoch) -> Result<(), PackError> {
        // Only a readable pre-v2 pack is migrated. If the data file cannot even be opened read-only
        // (an unwritten all-zero first write, or a corrupt data header), migration does not apply —
        // leave it to the normal writable open path, which reinitializes an unwritten file or
        // surfaces its own error. A pack already at v2 is likewise left untouched.
        let data_file = epochs_dir.join(format!("epoch-{epoch}")).join(Self::DATA_NAME);
        let is_legacy = matches!(
            Pack::<PackRecord>::open(&data_file, epoch as u64, true, PackCompression::ZStd, PACK_VERSION),
            Ok(p) if p.version() < SENTINEL_MIN_VERSION
        );
        if !is_legacy {
            return Ok(());
        }
        match Self::migrate_pack(epochs_dir, epoch, true)? {
            EpochMigrate::Migrated(what) => {
                info!(target: "consensus_pack", epoch, %what, "migrated legacy pack to v2 on open");
            }
            EpochMigrate::AlreadyCurrent => {}
            EpochMigrate::Corrupt(why) => return Err(PackError::CorruptPack(why)),
            // `apply == true` never returns a dry-run verdict.
            EpochMigrate::WouldMigrate(_) => {}
        }
        Ok(())
    }

    /// Build a fresh v2 pack (data log + indexes, sealed) in `migrate_dir` from the legacy `src`.
    /// Returns the number of outputs written.
    fn build_migrated_pack(
        src: &Pack<PackRecord>,
        version: u16,
        base_dir: &Path,
        data_file: &Path,
        epoch: Epoch,
        migrate_dir: &Path,
    ) -> Result<u64, MigrateAbort> {
        let dst_file = migrate_dir.join(Self::DATA_NAME);
        let mut dst = Pack::<PackRecord>::open(
            &dst_file,
            epoch as u64,
            false,
            PackCompression::ZStd,
            PACK_VERSION,
        )
        .map_err(|e| MigrateAbort::Fatal(e.into()))?;
        let copied = Self::migrate_copy(src, Some(&mut dst), version, base_dir, data_file, epoch)?;
        dst.commit().map_err(|e| MigrateAbort::Fatal(PackError::PersistError(e.to_string())))?;
        // Load the freshly-written epoch meta (the first record) for the `Inner` we build to
        // rebuild the indexes and seal.
        let epoch_meta = dst
            .fetch(DATA_HEADER_BYTES as u64)
            .map_err(|e| MigrateAbort::Fatal(PackError::ReadError(e.to_string())))?
            .into_epoch()
            .map_err(MigrateAbort::Fatal)?;
        // Rebuild the indexes from the fresh v2 log via the standard recovery path (empty indexes
        // -> full replay-rebuild), then persist + drop so every backing file gets its
        // clean-close sentinel. The log we just wrote is complete and header-first, so
        // recovery finds no torn tail; it simply repopulates the indexes.
        let (pos, cd, bd) = Self::open_indexes_for_append(migrate_dir, dst.header())
            .map_err(MigrateAbort::Fatal)?;
        let (pos, cd, bd) =
            Self::recover_pack(&mut dst, migrate_dir, pos, cd, bd).map_err(MigrateAbort::Fatal)?;
        let count = pos.len() as u64;
        // The rebuilt v2 pack must index exactly the outputs copied from the source: anything else
        // means the copy and the rebuild disagree, and the result must not replace the source.
        if count != copied {
            return Err(MigrateAbort::Fatal(PackError::CorruptPack(format!(
                "epoch {epoch}: migration copied {copied} output(s) but the rebuilt v2 pack indexes \
                 {count}; nothing was installed and the legacy pack is unchanged"
            ))));
        }
        let mut inner = Inner {
            data: dst,
            consensus_pos_idx: pos,
            consensus_digests: cd,
            batch_digests: bd,
            epoch_meta,
            #[cfg(test)]
            fail_save_after_append: false,
        };
        inner.persist().map_err(MigrateAbort::Fatal)?;
        // Drop seals every backing file (msync + truncate-to-`end` + clean-close sentinel).
        drop(inner);
        Ok(count)
    }

    /// Walk the legacy `src` data log record-by-record, validating as it reads. When `dst` is
    /// `Some`, each record is re-appended to the fresh v2 log in header-first order (reordering a
    /// v0 batches-first log). When `dst` is `None` this only validates and counts (the dry-run
    /// path). Returns the number of outputs.
    fn migrate_copy(
        src: &Pack<PackRecord>,
        mut dst: Option<&mut Pack<PackRecord>>,
        version: u16,
        base_dir: &Path,
        data_file: &Path,
        epoch: Epoch,
    ) -> Result<u64, MigrateAbort> {
        // For a header-first (v1) log, compute the safe consistent end with the same WAL replay +
        // committed-data guards recovery uses: an unacked crash tail past `end` is dropped, but a
        // committed output is never silently lost. A v0 (batches-first) log cannot be replayed; it
        // is validated by the reordering walk below and any decode failure is reported as
        // corruption (every v0 pack on disk is a sealed, complete historic epoch).
        let end = if version >= 1 {
            Self::migrate_consistent_end(src, base_dir, data_file, epoch)?
        } else {
            u64::MAX
        };
        let mut iter = src.raw_iter().map_err(|e| MigrateAbort::Fatal(DataFileOpen(e).into()))?;
        // The first record must be the epoch meta.
        match iter.next() {
            Some(Ok(PackRecord::EpochMeta(m))) => {
                if let Some(d) = dst.as_deref_mut() {
                    d.append(&PackRecord::EpochMeta(m))
                        .map_err(|e| MigrateAbort::Fatal(PackError::Append(e.to_string())))?;
                }
            }
            Some(Ok(_)) => {
                return Err(MigrateAbort::Corrupt(format!(
                    "epoch {epoch}: first record is not the epoch meta"
                )))
            }
            Some(Err(e)) => {
                return Err(MigrateAbort::Corrupt(format!(
                    "epoch {epoch}: epoch meta unreadable: {e}"
                )))
            }
            None => {
                return Err(MigrateAbort::Corrupt(format!(
                    "epoch {epoch}: empty pack (no epoch meta)"
                )))
            }
        }
        if version == 0 {
            Self::migrate_copy_v0(&mut iter, dst, epoch)
        } else {
            Self::migrate_copy_v1(&mut iter, dst, end, epoch)
        }
    }

    /// Compute the recoverable end of a header-first legacy log and reject any damage below the
    /// acked frontier (never a silent truncate of committed data). Mirrors
    /// [`Self::recover_pack`]'s pass-1 guards, plus a length-attestation seal test that stands
    /// in for the clean-close sentinel a pre-v2 pack never carried.
    fn migrate_consistent_end(
        src: &Pack<PackRecord>,
        base_dir: &Path,
        data_file: &Path,
        epoch: Epoch,
    ) -> Result<u64, MigrateAbort> {
        // A pre-v2 pack has no clean-close sentinel, so `replay_wal` (which reads "sealed" from the
        // sentinel) treats every legacy log as unclean and would truncate a damaged tail. Recover
        // the real completeness signal the pre-mmap build used: the cross-file LENGTH
        // attestation. If the source's own indexes still attest data-len == last output_end
        // == both digest lengths, the pack is COMPLETE (sealed-equivalent), so a replay
        // that stops short of that end is at-rest corruption of committed data — never a
        // truncatable tail. When the attestation does not hold (a current epoch's unacked
        // crash tail, or lagging/damaged indexes) we fall back to the tail-truncation logic
        // below, gated by the commit marker / position-index guards.
        let logically_sealed = match Self::try_open_indexes(base_dir, src.header(), true) {
            Ok((mut pos, cd, bd)) => Self::files_consistent(src, &mut pos, &cd, &bd),
            Err(_) => false,
        };
        let end = match Self::replay_wal(src, base_dir, None) {
            Ok(end) => end,
            Err(PackError::CorruptPack(m)) => return Err(MigrateAbort::Corrupt(m)),
            Err(e) => return Err(MigrateAbort::Fatal(e)),
        };
        if logically_sealed && end < src.file_len() {
            return Err(MigrateAbort::Corrupt(format!(
                "epoch {epoch}: a complete (length-consistent) legacy pack replays only to {end} of \
                 {}; committed data is damaged, not a truncatable tail. Re-sync the epoch from peers.",
                src.file_len()
            )));
        }
        if let Some(committed_end) = src.committed_end() {
            if end < committed_end {
                return Err(MigrateAbort::Corrupt(format!(
                    "epoch {epoch}: the data log replays only to {end} but a durable commit marker \
                     attests {committed_end}; committed data is damaged. Re-sync the epoch from peers."
                )));
            }
        }
        if attested_output_survives_past(data_file, epoch, end) {
            return Err(MigrateAbort::Corrupt(format!(
                "epoch {epoch}: a committed output starts past the recoverable end {end}; committed \
                 data is damaged. Re-sync the epoch from peers."
            )));
        }
        Ok(end)
    }

    /// Copy a header-first (v1) log up to `end`, output by output (`Consensus` header followed by
    /// the exact number of `Batch` records its sub-dag names). Records at/after `end` are the
    /// discarded unacked tail.
    fn migrate_copy_v1(
        iter: &mut crate::archive::pack::RawIter<PackRecord>,
        mut dst: Option<&mut Pack<PackRecord>>,
        end: u64,
        epoch: Epoch,
    ) -> Result<u64, MigrateAbort> {
        let mut count = 0u64;
        loop {
            if iter.logical_position() >= end {
                break;
            }
            match iter.next() {
                None => break,
                Some(Ok(PackRecord::Consensus(header))) => {
                    let expected = Self::expected_batch_count(&header);
                    if let Some(d) = dst.as_deref_mut() {
                        d.append(&PackRecord::Consensus(header))
                            .map_err(|e| MigrateAbort::Fatal(PackError::Append(e.to_string())))?;
                    }
                    for _ in 0..expected {
                        match iter.next() {
                            Some(Ok(PackRecord::Batch(batch))) => {
                                if let Some(d) = dst.as_deref_mut() {
                                    d.append(&PackRecord::Batch(batch)).map_err(|e| {
                                        MigrateAbort::Fatal(PackError::Append(e.to_string()))
                                    })?;
                                }
                            }
                            Some(Ok(_)) => {
                                return Err(MigrateAbort::Corrupt(format!(
                                    "epoch {epoch}: output {count} expected a batch record"
                                )))
                            }
                            Some(Err(e)) => {
                                return Err(MigrateAbort::Corrupt(format!(
                                    "epoch {epoch}: output {count} batch decode failed: {e}"
                                )))
                            }
                            None => {
                                return Err(MigrateAbort::Corrupt(format!(
                                    "epoch {epoch}: output {count} truncated before its batches"
                                )))
                            }
                        }
                    }
                    count += 1;
                }
                Some(Ok(_)) => {
                    return Err(MigrateAbort::Corrupt(format!(
                        "epoch {epoch}: unexpected record where a consensus header was expected"
                    )))
                }
                Some(Err(e)) => {
                    return Err(MigrateAbort::Corrupt(format!(
                        "epoch {epoch}: record decode failed: {e}"
                    )))
                }
            }
        }
        Ok(count)
    }

    /// Copy a v0 batches-first log, reordering each output into header-first form: batches are
    /// buffered until their `Consensus` header appears, then the header and its batches are
    /// appended. Any decode failure or a batch count that disagrees with the header is
    /// corruption.
    fn migrate_copy_v0(
        iter: &mut crate::archive::pack::RawIter<PackRecord>,
        mut dst: Option<&mut Pack<PackRecord>>,
        epoch: Epoch,
    ) -> Result<u64, MigrateAbort> {
        let mut count = 0u64;
        let mut pending: Vec<Batch> = Vec::new();
        loop {
            match iter.next() {
                None => {
                    if !pending.is_empty() {
                        return Err(MigrateAbort::Corrupt(format!(
                            "epoch {epoch}: {} trailing batch record(s) with no consensus header (v0)",
                            pending.len()
                        )));
                    }
                    break;
                }
                Some(Ok(PackRecord::Batch(batch))) => pending.push(batch),
                Some(Ok(PackRecord::Consensus(header))) => {
                    let expected = Self::expected_batch_count(&header);
                    if pending.len() != expected {
                        return Err(MigrateAbort::Corrupt(format!(
                            "epoch {epoch}: v0 output {count} has {} batch record(s) but its header \
                             names {expected}",
                            pending.len()
                        )));
                    }
                    if let Some(d) = dst.as_deref_mut() {
                        d.append(&PackRecord::Consensus(header))
                            .map_err(|e| MigrateAbort::Fatal(PackError::Append(e.to_string())))?;
                        for batch in pending.drain(..) {
                            d.append(&PackRecord::Batch(batch)).map_err(|e| {
                                MigrateAbort::Fatal(PackError::Append(e.to_string()))
                            })?;
                        }
                    } else {
                        pending.clear();
                    }
                    count += 1;
                }
                Some(Ok(PackRecord::EpochMeta(_))) => {
                    return Err(MigrateAbort::Corrupt(format!(
                        "epoch {epoch}: a second epoch-meta record (v0)"
                    )))
                }
                Some(Err(e)) => {
                    return Err(MigrateAbort::Corrupt(format!(
                        "epoch {epoch}: v0 record decode failed: {e}"
                    )))
                }
            }
        }
        Ok(count)
    }

    /// Atomically install a freshly-built `epoch-{epoch}.migrating` dir at `epochs_dir`, replacing
    /// the live `epoch-{epoch}` via rename-aside: the old dir is moved to `epoch-{epoch}.replaced`
    /// and only removed after the new one is renamed in and the parent is fsync'd. On a rename
    /// failure the old dir is restored. A crash mid-swap leaves a `*.replaced` (and possibly the
    /// `*.migrating`) dir that startup cleanup sweeps. Mirrors
    /// `ConsensusStore::install_imported_epoch_dir`.
    fn install_migrated_dir(
        epochs_dir: &Path,
        epoch: Epoch,
        migrate_dir: &Path,
    ) -> Result<(), PackError> {
        let base_dir = epochs_dir.join(format!("epoch-{epoch}"));
        let aside = epochs_dir.join(format!("epoch-{epoch}.replaced"));
        install_dir_rename_aside(epochs_dir, &base_dir, &aside, migrate_dir)?;
        Ok(())
    }

    /// Prefix of the directory, inside an epoch dir, where a read-side index rebuild is staged
    /// before it replaces the live index directories. Each build stages in its own directory
    /// ([`Self::heal_staging_name`]), so no build can touch (or remove) another build's staging.
    const REINDEX_DIR: &str = ".reindex";

    /// A staging directory name unique to one heal build: `{prefix}-{n}` from a process-wide
    /// counter (with `suffix` appended). Staging is swept at startup by prefix/suffix, so what a
    /// crash left mid-build or mid-install is removed on the next start.
    fn heal_staging_name(prefix: &str, suffix: &str) -> String {
        static HEAL_BUILD_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = HEAL_BUILD_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}-{n}{suffix}")
    }
    /// Directory, inside an epoch dir, where the replaced index directories are parked during a
    /// read-side index swap.
    const REINDEX_OLD_DIR: &str = ".reindex-old";

    /// The derived index directories of an epoch pack.
    const INDEX_DIRS: [&str; 3] =
        [Self::CONSENSUS_POS_NAME, Self::CONSENSUS_HASH_NAME, Self::BATCH_HASH_NAME];

    /// Rebuild a cleanly sealed v2 pack's derived indexes from its WAL into [`Self::REINDEX_DIR`]
    /// inside `base_dir`, opening the data log READ-ONLY, and return that directory. The live
    /// index directories are untouched until [`Self::install_static_indexes`].
    ///
    /// The WAL is validated on its own first (`replay_wal` pass 1, nothing written); a sealed log
    /// with any tear is `CorruptPack`. The fresh indexes are synced and sealed before returning.
    fn build_static_indexes(
        base_dir: &Path,
        data_file: &Path,
        epoch: Epoch,
    ) -> Result<PathBuf, PackError> {
        let data = Pack::<PackRecord>::open(
            data_file,
            epoch as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        if data.opened_unclean() || data.version() < SENTINEL_MIN_VERSION {
            return Err(Self::corrupt_pack(base_dir));
        }
        let side = base_dir.join(Self::heal_staging_name(Self::REINDEX_DIR, ""));
        create_dir_synced(&side)?;
        let built = Self::rebuild_indexes_into(&data, base_dir, &side);
        if built.is_err() {
            let _ = std::fs::remove_dir_all(&side);
        }
        built.map(|()| side)
    }

    /// Replay the (read-only) `data` log of the pack in `base_dir` into fresh indexes created under
    /// `side`, then sync and seal them. See [`Self::build_static_indexes`].
    fn rebuild_indexes_into(
        data: &Pack<PackRecord>,
        base_dir: &Path,
        side: &Path,
    ) -> Result<(), PackError> {
        let end = Self::replay_wal(data, base_dir, None)?;
        if end != data.file_len() {
            return Err(Self::corrupt_pack(base_dir));
        }
        let mut consensus_pos_idx = Self::open_pdx_file(side, data.header(), false)?;
        let (mut consensus_digests, mut batch_digests) =
            Self::open_digest_indexes(side, data.header(), false)?;
        Self::replay_wal(
            data,
            base_dir,
            Some((&mut consensus_pos_idx, &mut consensus_digests, &mut batch_digests)),
        )?;
        consensus_digests.set_data_file_length(end);
        batch_digests.set_data_file_length(end);
        let persist = |e: &dyn Display| PackError::PersistError(e.to_string());
        consensus_pos_idx.sync().map_err(|e| persist(&e))?;
        consensus_digests.sync().map_err(|e| persist(&e))?;
        batch_digests.sync().map_err(|e| persist(&e))?;
        // Dropping the freshly created indexes seals them (clean-close sentinel + fsync).
        drop((consensus_pos_idx, consensus_digests, batch_digests));
        for name in Self::INDEX_DIRS {
            fsync_directory(&side.join(name))?;
        }
        fsync_directory(side)?;
        Ok(())
    }

    /// Swap indexes rebuilt by [`Self::build_static_indexes`] (in `side`) in for the live index
    /// directories of the pack in `base_dir`: the live ones are moved into
    /// [`Self::REINDEX_OLD_DIR`], the rebuilt ones renamed into place, and the epoch dir fsync'd.
    /// Renames only: nothing is truncated in place, so a reader still mapping an old index keeps
    /// a valid (unlinked) mapping, and the data log is never touched. A crash mid-swap leaves index
    /// directories missing, which the next read rebuilds again. The position index is moved out
    /// last and back in first, so no crash point leaves it the only missing directory (a fresh,
    /// empty position index beside intact digest indexes is the one shape the writable open must
    /// recognise as lost rather than new; the digest markers are what force that rebuild).
    fn install_static_indexes(base_dir: &Path, side: &Path) -> Result<(), PackError> {
        let old = base_dir.join(Self::REINDEX_OLD_DIR);
        let _ = std::fs::remove_dir_all(&old);
        create_dir_synced(&old)?;
        for name in Self::INDEX_DIRS.iter().rev() {
            let live = base_dir.join(name);
            if live.exists() {
                std::fs::rename(&live, old.join(name))?;
            }
        }
        for name in Self::INDEX_DIRS {
            std::fs::rename(side.join(name), base_dir.join(name))?;
        }
        fsync_directory(base_dir)?;
        let _ = std::fs::remove_dir_all(&old);
        let _ = std::fs::remove_dir_all(side);
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
        let consensus_pos_idx = PositionIndex::open_pdx_file(
            &base_dir,
            data_header,
            Self::CONSENSUS_POS_FILE,
            read_only,
        )
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
        let consensus_digests =
            Self::open_hdx(base_dir, Self::CONSENSUS_HASH_NAME, data_header, read_only)?;
        let batch_digests =
            Self::open_hdx(base_dir, Self::BATCH_HASH_NAME, data_header, read_only)?;
        Ok((consensus_digests, batch_digests))
    }

    /// Open (creating if empty and writable) one digest index, in directory `name` under
    /// `base_dir`.
    fn open_hdx(
        base_dir: &Path,
        name: &str,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<HdxIndex, PackError> {
        Ok(HdxIndex::open_hdx_file(
            base_dir.join(name),
            data_header,
            BuildHasherDefault::<FxHasher>::default(),
            read_only,
        )
        .map_err(OpenError::IndexFileOpen)?)
    }

    /// Open all three of an epoch's indexes for append, discarding (and recreating empty) any one
    /// whose contents are broken so the caller's [`Self::recover_pack`] rebuilds it from the data
    /// log.
    ///
    /// The data log is the source of truth and the indexes are always reconstructable from it, so
    /// an index that exists but will not open (a corrupt or short header, or a version/uid/
    /// geometry/hasher mismatch) must not abort the open (INV3). Only the index that failed is
    /// discarded: in particular a readable position index survives, and with it the attested
    /// output boundaries `recover_pack` uses to tell a torn tail from damage to acked data
    /// ([`Self::attested_record_survives`]). A failure that says nothing about the index's contents
    /// — the environment rather than the file, e.g. descriptor or memory exhaustion or a permission
    /// error — is returned instead of discarding an index that may be intact.
    fn open_indexes_for_append(
        base_dir: &Path,
        data_header: &DataHeader,
    ) -> Result<(PositionIndex<IndexPositions>, HdxIndex, HdxIndex), PackError> {
        let mut discarded = false;
        let consensus_pos_idx = Self::open_index_for_append(
            base_dir,
            Self::CONSENSUS_POS_NAME,
            &mut discarded,
            || Self::open_pdx_file(base_dir, data_header, false),
        )?;
        let mut consensus_digests = Self::open_index_for_append(
            base_dir,
            Self::CONSENSUS_HASH_NAME,
            &mut discarded,
            || Self::open_hdx(base_dir, Self::CONSENSUS_HASH_NAME, data_header, false),
        )?;
        let mut batch_digests =
            Self::open_index_for_append(base_dir, Self::BATCH_HASH_NAME, &mut discarded, || {
                Self::open_hdx(base_dir, Self::BATCH_HASH_NAME, data_header, false)
            })?;
        if discarded {
            // A discarded index comes back empty while the survivors still attest the whole log,
            // which `files_consistent` could accept (e.g. an empty position index next to intact
            // digest indexes). Invalidate the digest commit markers — `0` never equals a real data
            // length — so `recover_pack` rebuilds every index from the WAL.
            consensus_digests.set_data_file_length(0);
            batch_digests.set_data_file_length(0);
        }
        Ok((consensus_pos_idx, consensus_digests, batch_digests))
    }

    /// Open one index for append via `open`; if its contents are broken, remove its directory
    /// `name` under `base_dir`, open it again empty, and set `discarded`. See
    /// [`Self::open_indexes_for_append`].
    fn open_index_for_append<T>(
        base_dir: &Path,
        name: &str,
        discarded: &mut bool,
        open: impl Fn() -> Result<T, PackError>,
    ) -> Result<T, PackError> {
        match open() {
            Ok(index) => Ok(index),
            Err(e) if e.is_environmental_index_error() => Err(e),
            Err(e) => {
                *discarded = true;
                warn!(
                    target: "consensus::pack",
                    "epoch pack {} index `{name}` failed to open ({e}); discarding it to rebuild \
                     from the data log",
                    base_dir.display(),
                );
                match std::fs::remove_dir_all(base_dir.join(name)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                open()
            }
        }
    }

    /// Open all three indexes, creating any that are missing when writable and returning an error
    /// if an existing index will not open. The fallible counterpart to
    /// [`Self::open_indexes_for_append`]'s discard-and-rebuild fallback (also used to reopen the
    /// freshly emptied index after [`Self::open_index_for_append`] wipes it) and to
    /// [`Self::open_indexes_static`]'s read-only door.
    fn try_open_indexes(
        base_dir: &Path,
        data_header: &DataHeader,
        read_only: bool,
    ) -> Result<(PositionIndex<IndexPositions>, HdxIndex, HdxIndex), PackError> {
        let consensus_pos_idx = Self::open_pdx_file(base_dir, data_header, read_only)?;
        let (consensus_digests, batch_digests) =
            Self::open_digest_indexes(base_dir, data_header, read_only)?;
        Ok((consensus_pos_idx, consensus_digests, batch_digests))
    }

    /// Open all three of a *sealed* epoch's indexes read-only. A read-only door cannot rebuild an
    /// index (see the `open_static` contract), so a damaged one is terminal -- but it is surfaced
    /// with the actionable [`Self::corrupt_static_index`] remediation rather than a bare
    /// `LoadHeaderError`. A genuinely *absent* index file keeps its `NotFound` classification (a
    /// clean miss) so [`PackError::is_missing_static_files`] still holds for the staging-window
    /// race.
    fn open_indexes_static(
        base_dir: &Path,
        epoch: Epoch,
        data_header: &DataHeader,
    ) -> Result<(PositionIndex<IndexPositions>, HdxIndex, HdxIndex), PackError> {
        match Self::try_open_indexes(base_dir, data_header, true) {
            Ok(indexes) => Ok(indexes),
            Err(e) if e.is_missing_static_files() => Err(e),
            Err(e) => Err(Self::corrupt_static_index(base_dir, epoch, &e)),
        }
    }

    /// Opens a new epoch pack for append.  Will create a new set of epoch static
    /// files to write consensus output into if they do not exist.
    ///
    /// A data file holding record bytes must begin with a readable [`EpochMeta`] matching this
    /// epoch.  An unreadable first record fails the open (an invalid pack) rather than repairing
    /// it: the meta is committed the instant it is written (msync'd, its size extension fsync'd),
    /// so a valid pack always has a durable meta and a torn/missing meta is not a recoverable
    /// state. A header-only file (no meta yet) is initialized by writing and committing the meta.
    ///
    /// "Matching" is decided by [`EpochMeta::authenticated_mismatch`] against the meta derived from
    /// `previous_epoch` and `committee`, and covers only the six fields [`verify_epoch_meta`]
    /// authenticates on import: the record's and its committee's epoch, the start consensus number,
    /// the genesis execution and consensus states, and the committee's BLS key set. The key set is
    /// compared against `committee`'s, not against `previous_epoch.next_committee` as import does,
    /// because `new_epoch` does not guarantee the two sets are equal. The rest of the on-disk
    /// committee is the serving peer's copy when the epoch was imported, so it is not compared. A
    /// mismatch fails the open before any meta is written or index opened; on success this open
    /// carries the chain-derived meta, never the one read from disk, though a later
    /// [`Self::open_append_exists`] or [`Self::open_static`] of the epoch loads the on-disk one.
    fn open_append<P: AsRef<Path>>(
        path: P,
        previous_epoch: &EpochRecord,
        committee: Committee,
        version: u16,
    ) -> Result<Self, PackError> {
        let epoch = committee.epoch();
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        create_dir_synced(&base_dir)?;
        let pack_file = base_dir.join(Self::DATA_NAME);
        let have_pack = std::fs::exists(&pack_file).unwrap_or_default();
        // Migrate a pre-sentinel legacy pack up to v2 before we ever write to it. Skipped when the
        // caller explicitly requests a legacy `version` (the test helper that *creates* v0/v1
        // packs); production always requests `PACK_VERSION`.
        if have_pack && version >= SENTINEL_MIN_VERSION {
            Self::migrate_legacy_if_needed(path.as_ref(), epoch)?;
        }
        let mut data: Pack<PackRecord> =
            Pack::open(&pack_file, epoch as u64, false, PackCompression::ZStd, version)?;
        let epoch_meta = EpochMeta {
            epoch,
            committee,
            start_consensus_number: epoch_start_consensus_number(epoch, previous_epoch),
            genesis_exec_state: previous_epoch.final_state,
            genesis_consensus: previous_epoch.final_consensus,
        };

        // Set by the header-only branch, which writes a fresh meta at the data header.  The pack is
        // then byte-for-byte a freshly created one, so it needs the same index initialization.
        let mut wrote_fresh_meta = false;

        // Detect a present meta by the first record's 4-byte length prefix, NOT by the file length.
        // A crash between the header write and the first meta append leaves an unclean file grown
        // to its mmap capacity and zero-padded: `file_len()` (physical) would then exceed
        // the header even though no meta was ever written, and a plain length check
        // mistakes that padding for a torn meta and fatally rejects a pack that only needs
        // its meta initialized. A real record's length prefix is non-zero and
        // `record_present_at` reads it within the logical bounds, so a cleanly-sealed
        // header-only file (end == header) reports "no meta" too.
        let pack_len = data.file_len();
        if data.record_present_at(DATA_HEADER_BYTES as u64) {
            match data.fetch(DATA_HEADER_BYTES as u64) {
                Ok(record) => {
                    let meta = record.into_epoch()?;
                    // an imported pack stores the serving peer's committee, authenticated only by
                    // its bls key set, so a full comparison would refuse a correctly imported
                    // epoch. the opened pack keeps the chain-derived `epoch_meta` either way.
                    if let Some(mismatch) = epoch_meta.authenticated_mismatch(&meta) {
                        return Err(PackError::InvalidEpoch(
                            epoch,
                            format!("open append has unexpected meta data: {mismatch}"),
                        ));
                    }
                }
                Err(e) => {
                    // A data file holding record bytes must begin with a readable meta. An
                    // unreadable first record is an invalid pack: recovery (`recover_pack`) only
                    // trims the torn tail, and any records behind the meta stay addressable through
                    // the indexes, so nothing here can safely repair it. The meta is committed the
                    // moment it is written (below and in `stream_import`), so a torn meta is not a
                    // normal state -- fail rather than rewrite it.
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
            }
        } else {
            // `record_present_at` is false: the meta's 4-byte length prefix reads as zero. That is
            // the shape of a brand-new or header-only file -- but it is ALSO the shape of an
            // occupied pack whose meta length-prefix was corrupted to zero, where the meta payload
            // and every committed output still sit past the header as non-zero bytes. Truncating to
            // the header (below) would silently erase them, so first prove the region past the
            // header is genuinely empty. Any content there means a real (if now unreadable) meta
            // over live data: reject and preserve the pack -- the same fail-closed treatment as
            // `open_append_exists` and the torn-prefix path above -- rather than re-initialize.
            if data.any_content_after(DATA_HEADER_BYTES as u64) {
                return Err(PackError::EpochLoad(format!(
                    "epoch {epoch} pack {} ({pack_len} bytes): the epoch meta's length prefix is \
                     zeroed but committed data remains past the header -- refusing to \
                     re-initialize, which would erase it. The data is intact; inspect it with \
                     `telcoin-network db validate {}` and re-sync this epoch from peers. Do NOT \
                     delete this `epoch-{epoch}` directory or the chain-data directories (`db`, \
                     `static_files`, `consensus-db`)",
                    pack_file.display(),
                    base_dir.display(),
                )));
            }
            // Genuinely header-only: brand new, or a crash landed between the header write and the
            // meta append.  A crash can leave the file grown to its mmap capacity and zero-padded
            // past the header, so roll the logical end back to exactly the header first -- the meta
            // must be the first record at DATA_HEADER_BYTES, never after the padding. Commit
            // immediately so the header+meta prefix is durable before we return: a valid pack
            // always has a durable meta, which is what lets the torn-meta path above
            // fail instead of repair.
            if pack_len > DATA_HEADER_BYTES as u64 {
                data.rewind_to(DATA_HEADER_BYTES as u64);
            }
            data.append(&PackRecord::EpochMeta(epoch_meta.clone()))
                .map_err(|e| PackError::Append(e.to_string()))?;
            data.commit().map_err(|e| PackError::PersistError(e.to_string()))?;
            wrote_fresh_meta = true;
        }
        // The data file and its epoch meta are now established and durable -- the parts that are
        // not repairable. Only now open the indexes, rebuilding all of them from the data log if
        // any is broken, so an index problem can never abort the open.
        let (consensus_pos_idx, mut consensus_digests, mut batch_digests) =
            Self::open_indexes_for_append(&base_dir, data.header())?;
        if !have_pack || wrote_fresh_meta {
            // A new DB, or a header-only file that just had its meta written, needs the index file
            // lengths initialized to the current data length. A header-only reopen counts as new:
            // the indexes may carry a stale length, and leaving that would make recovery truncate
            // the meta we just wrote back down to it -- returning a live pack with a torn meta.
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
        Ok(Self {
            data,
            consensus_digests,
            consensus_pos_idx,
            batch_digests,
            epoch_meta,
            #[cfg(test)]
            fail_save_after_append: false,
        })
    }

    /// Open up the files for previous epoch in append mode.  Will fail if files do not exist.
    fn open_append_exists<P: AsRef<Path>>(path: P, epoch: Epoch) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        let pack_file = base_dir.join(Self::DATA_NAME);

        // This door opens an epoch that must already exist; it must not create anything. A
        // read-write `Pack::open` would create the data file if it were missing (leaving a stray
        // empty file and a misleading "meta unreadable" error), so fail up front with a
        // NotFound-classified error that `PackError::is_missing_static_files` recognizes as a clean
        // miss.
        if !std::fs::exists(&pack_file).unwrap_or(false) {
            return Err(PackError::Open(Arc::new(DataFileOpen(LoadHeaderError::IO(
                io::Error::new(io::ErrorKind::NotFound, pack_file.display().to_string()),
            )))));
        }

        // v2 is the only writable format: migrate a pre-sentinel legacy pack up before opening it
        // for append (this door reopens an existing epoch, so it is the path a node takes
        // to continue writing a current epoch that was last written by a pre-mmap build).
        Self::migrate_legacy_if_needed(path.as_ref(), epoch)?;

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
        // The meta's own epoch must match the directory it was loaded from. This is bound
        // implicitly by the data-header uid (`gen_uid(epoch)`, checked in `Pack::open`); assert it
        // explicitly so a future change to that derivation cannot silently let a mislabeled meta
        // drive `save_consensus_output`/range checks under the wrong epoch.
        if epoch_meta.epoch != epoch {
            return Err(PackError::InvalidEpoch(
                epoch,
                format!(
                    "on-disk epoch meta is for epoch {} but opened as {epoch}",
                    epoch_meta.epoch
                ),
            ));
        }
        // The data file and its epoch meta are established. Open the indexes, rebuilding all of
        // them from the data log if any is broken so an index problem can never abort the
        // open.
        let (consensus_pos_idx, consensus_digests, batch_digests) =
            Self::open_indexes_for_append(&base_dir, data.header())?;

        // Rebuild the indexes from the data-log WAL and truncate any torn tail record.
        let (consensus_pos_idx, consensus_digests, batch_digests) = Self::recover_pack(
            &mut data,
            &base_dir,
            consensus_pos_idx,
            consensus_digests,
            batch_digests,
        )?;
        Ok(Self {
            data,
            consensus_digests,
            consensus_pos_idx,
            batch_digests,
            epoch_meta,
            #[cfg(test)]
            fail_save_after_append: false,
        })
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
        // See `open_append_exists`: the meta's epoch is bound implicitly by the data-header uid;
        // assert it explicitly here too.
        if epoch_meta.epoch != epoch {
            return Err(PackError::InvalidEpoch(
                epoch,
                format!(
                    "on-disk epoch meta is for epoch {} but opened as {epoch}",
                    epoch_meta.epoch
                ),
            ));
        }
        // Read-only: a damaged index is terminal (this door cannot rebuild it), but surface the
        // actionable remediation instead of a bare LoadHeaderError; a missing index stays a clean
        // miss.
        let (mut consensus_pos_idx, consensus_digests, batch_digests) =
            Self::open_indexes_static(&base_dir, epoch, data.header())?;

        if !Self::files_consistent(
            &data,
            &mut consensus_pos_idx,
            &consensus_digests,
            &batch_digests,
        ) {
            // Corrupt static file is bad (damaged at rest?), produce an error. Read-only opens do
            // not heal, so this is terminal. When it is the position index's own last entry that
            // fails its CRC, the index is what is damaged: give its remediation (rebuild it from
            // the log); otherwise the same remediation as the recovery path.
            if let Some(last) = consensus_pos_idx.len().checked_sub(1) {
                if let Err(e) = consensus_pos_idx.load(last as u64) {
                    return Err(Self::corrupt_static_index(&base_dir, epoch, &e.into()));
                }
            }
            return Err(Self::corrupt_pack(&base_dir));
        }
        // Clamp the read-only data handle's read bound to the index-attested committed length.
        // `files_consistent` just proved `data.file_len() == data_file_length` (a sealed pack has
        // no capacity padding), so this is a no-op today — but it makes the read bound
        // provably the attested end, so a read can never touch bytes a writer truncation
        // would remove even if a padded file ever reached here. Defense-in-depth for the
        // read-only-mmap SIGBUS window.
        let attested = consensus_digests.data_file_length();
        debug_assert_eq!(
            data.file_len(),
            attested,
            "a sealed pack's physical length must equal the index-attested length"
        );
        data.set_read_bound(attested);
        Ok(Self {
            data,
            consensus_digests,
            consensus_pos_idx,
            batch_digests,
            epoch_meta,
            #[cfg(test)]
            fail_save_after_append: false,
        })
    }

    /// Create a new set of epoch static files and fill them from a peer's pack `stream`.
    ///
    /// The imported epoch is always written in the CURRENT format (`PACK_VERSION`): v2 is the only
    /// writable format, so an import never lands on disk as a legacy pack that a later read would
    /// have to migrate. A v1/v2 (header-first) source is streamed into the pack record by record
    /// ([`Self::import_streamed_output`]). A v0 (batches-first) source is refused
    /// (`InvalidVersion`, no penalty): v0 is only ever migrated on disk, and an upgraded peer
    /// serves its v0 epochs migrated.
    ///
    /// Nothing in the stream is authenticated until the chain reaches the certified final (checked
    /// by the caller), so an output's header is only parent-linked here. Streaming keeps the
    /// decoded memory for a hostile header that declares a huge batch fan-out bounded by a
    /// single record rather than by the (committee-scaled, multi-GB) theoretical maximum output
    /// size. Reading stops at `final_consensus_number`: anything a peer streams past it cannot
    /// belong to this epoch's certified chain.
    /// Import a full epoch (or a verifiable prefix) from a peer `stream` into a fresh pack under
    /// `path`, stopping before the filesystem's free space drops below `min_free` (see
    /// [`IMPORT_MIN_FREE_BYTES`]).
    async fn stream_import<P: AsRef<Path>, R: AsyncRead + Unpin>(
        path: P,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
        min_free: u64,
    ) -> Result<Self, PackError> {
        let base_dir = path.as_ref().join(format!("epoch-{epoch}"));
        create_dir_synced(&base_dir)?;
        let mut floor = DiskFloor { dir: base_dir.clone(), min_free, checked_at: None };
        floor.check(0)?;
        // `AsyncPackIter::open` rejects a source newer than `PACK_VERSION` (its `max_version`).
        // A header that does not read (transport) or is from a newer build is no fault of the
        // sender's bytes; one that reads but is wrong (a failed CRC, another epoch's uid, a
        // foreign app number), or a stream that ends before its header is complete (see
        // `next_output_record`), is.
        let mut stream_iter =
            AsyncPackIter::<PackRecord, R>::open(stream, epoch as u64, PACK_VERSION)
                .await
                .map_err(|e| match e {
                    LoadHeaderError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        PackError::UndecodableRecord(format!("stream header truncated: {err}"))
                    }
                    LoadHeaderError::IO(_) | LoadHeaderError::InvalidVersion => {
                        PackError::ReadError(e.to_string())
                    }
                    _ => PackError::UndecodableRecord(format!("stream header: {e}")),
                })?;
        // A legacy v0 (batches-first) pack is only ever migrated on disk, never imported: an
        // upgraded peer serves its v0 epochs migrated, so refuse the source before writing
        // anything. An older build's honest bytes are no fault of the peer's (no penalty). A v1
        // source has v2's layout and imports like one.
        if stream_iter.version() == 0 {
            return Err(PackError::InvalidVersion(PACK_VERSION, 0));
        }
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
        // Commit the meta immediately so the header+meta prefix is durable: a valid pack always has
        // a durable meta, so a crash mid-import leaves a readable meta (or a header-only file that
        // reinitializes), never a torn meta that open would reject.
        data.commit().map_err(|e| PackError::PersistError(e.to_string()))?;
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
        let mut pack = Self {
            data,
            consensus_pos_idx,
            consensus_digests,
            batch_digests,
            epoch_meta,
            #[cfg(test)]
            fail_save_after_append: false,
        };
        // Fill the pack from the stream. Any error abandons the partial pack cheaply: a peer can
        // stream data and then send a bad record, and letting `pack` drop normally would msync +
        // seal (sentinel + fsync) the whole partial import on the tokio worker right before
        // `ImportPath::drop` deletes it. `discard_import` marks every backing file remove-on-drop
        // so the drop is cheap.
        let fill: Result<(), PackError> = 'fill: {
            loop {
                // Each output's header is checked (parent link, number, final bound, batch fan-out)
                // BEFORE any of its batches is read; `parent_digest_expectation` then advances to
                // this output's digest for the next one.
                let imported = pack
                    .import_streamed_output(
                        &mut stream_iter,
                        timeout,
                        parent_digest_expectation,
                        final_consensus_number,
                        &mut floor,
                    )
                    .await;
                match imported {
                    // Clean end of stream: no further output header.
                    Ok(None) => break,
                    // The requested final is in: stop reading. The caller verifies its digest
                    // against the certified record.
                    Ok(Some((_, number))) if number == final_consensus_number => break,
                    Ok(Some((digest, _))) => {
                        parent_digest_expectation = HeaderExpectation::Parent(digest)
                    }
                    Err(e) => break 'fill Err(e),
                }
            }
            Ok(())
        };
        if let Err(e) = fill {
            pack.discard_import();
            return Err(e);
        }
        Ok(pack)
    }

    /// Abandon this partial, never-persisted import pack: mark every backing file to be removed
    /// (not sealed) when it drops, so a failed/aborted `stream_import`'s drop skips the msync +
    /// truncate + clean-close sentinel + fsync of data that is about to be deleted anyway (the
    /// caller's `ImportPath::drop` removes the directory).
    fn discard_import(&mut self) {
        self.data.set_remove_on_drop();
        self.consensus_digests.set_remove_on_drop();
        self.batch_digests.set_remove_on_drop();
        self.consensus_pos_idx.set_remove_on_drop();
    }

    /// Import the next output of a v1/v2 (header-first) peer stream straight into this import pack
    /// without ever buffering the output.
    ///
    /// The header is read and checked first ([`check_header_expectation`] and
    /// [`Self::check_import_header`]); then each batch is verified against the header's next
    /// declared digest and the per-batch size cap and appended as it arrives. Decoded memory is
    /// therefore one record, however large a batch fan-out the (not yet authenticated) header
    /// declares. Returns the output's digest and number, or `None` at a clean end of stream.
    /// Any error leaves a partial output behind; the caller discards the whole import pack.
    /// `floor` is checked as the pack grows.
    async fn import_streamed_output<R: AsyncRead + Unpin>(
        &mut self,
        stream_iter: &mut AsyncPackIter<PackRecord, R>,
        timeout: Duration,
        expectation: HeaderExpectation,
        final_consensus_number: u64,
        floor: &mut DiskFloor,
    ) -> Result<Option<(ConsensusHeaderDigest, u64)>, PackError> {
        let header = match next_output_record(stream_iter, timeout).await? {
            None => return Ok(None),
            Some(PackRecord::Consensus(header)) => *header,
            Some(PackRecord::EpochMeta(_)) => {
                return Err(PackError::UnexpectedRecord(
                    "unexpected epoch meta data found".to_string(),
                ))
            }
            Some(PackRecord::Batch(_)) => {
                return Err(PackError::UnexpectedRecord("unexpected batch found".to_string()))
            }
        };
        check_header_expectation(&header, expectation)?;
        let (consensus_idx, declared) =
            self.check_import_header(&header, final_consensus_number)?;
        let (header_pos, digest, number) = self.append_imported_header(header)?;
        floor.check(self.data.file_len())?;
        let max_bytes = max_batch_size(self.epoch_meta.committee.epoch());
        for expected in declared {
            let batch = match next_output_record(stream_iter, timeout).await? {
                Some(PackRecord::Batch(batch)) => batch,
                None => return Err(PackError::MissingBatch),
                Some(PackRecord::EpochMeta(_)) => {
                    return Err(PackError::UnexpectedRecord(
                        "unexpected epoch meta data found".to_string(),
                    ))
                }
                Some(PackRecord::Consensus(_)) => {
                    return Err(PackError::UnexpectedRecord(
                        "unexpected consensusheader found".to_string(),
                    ))
                }
            };
            let got = batch.digest();
            if got != expected {
                return Err(PackError::UnexpectedRecord(format!(
                    "unexpected batch found, expected {expected}, got {got}"
                )));
            }
            // Same per-batch byte cap the batch validator enforces at production/gossip.
            let batch_bytes = batch.transactions.iter().map(|tx| tx.len()).sum::<usize>();
            if batch_bytes > max_bytes {
                return Err(PackError::BatchTooLarge { size: batch_bytes, max: max_bytes });
            }
            self.append_imported_batch(got, batch)?;
            floor.check(self.data.file_len())?;
        }
        self.finish_imported_output(consensus_idx, header_pos)?;
        Ok(Some((digest, number)))
    }

    /// The strict-in-order checks for the next imported output, run on its header before any of its
    /// batches is read. Returns the output's index in this pack and its declared (dedup'd, sorted)
    /// batch digests: exactly the batch records, in order, the header-first layout stores after it.
    ///
    /// - A number that is not exactly the next one is `InvalidConsensusNumber`, which is peer
    ///   misbehavior, whatever the number. A streamed import builds a fresh pack strictly in order;
    ///   accepting a repeat (the idempotent local-replay no-op) would let a non-advancing
    ///   parent-linked chain pin the import forever.
    /// - The next number, but over the requester's final, is `ConsensusNumberTooHigh`, which
    ///   charges no penalty. `final_consensus_number` is LOCAL state (the requester's epoch
    ///   record), and the next output lying past it most likely means that record is stale (e.g. a
    ///   not-yet-repaired dummy `0`). Once the final itself is imported the caller stops reading,
    ///   so a peer cannot append past it.
    /// - An output from another epoch is `InvalidEpoch`, and a fan-out beyond what a legitimate
    ///   output can reference is `TooManyBatches`. Every certificate author of an output with
    ///   batches must be in the epoch's committee, as the output decode requires to attribute them;
    ///   one that is not is the sender's bytes (`UnexpectedRecord`), not a local committee
    ///   mismatch.
    ///
    /// `header` must already have passed [`check_header_expectation`], which rejects the empty
    /// (leaderless) sub-dag that `leader_epoch()` would panic on.
    fn check_import_header(
        &self,
        header: &ConsensusHeader,
        final_consensus_number: u64,
    ) -> Result<(u64, BTreeSet<BlockHash>), PackError> {
        let number = header.number;
        let start = self.epoch_meta.start_consensus_number;
        let expected = start + self.consensus_pos_idx.len() as u64;
        if number != expected {
            return Err(PackError::InvalidConsensusNumber(expected, number));
        }
        if number > final_consensus_number {
            return Err(PackError::ConsensusNumberTooHigh);
        }
        let epoch = header.sub_dag.leader_epoch();
        if epoch != self.epoch_meta.epoch {
            return Err(PackError::InvalidEpoch(
                epoch,
                format!(
                    "Tried to import output from epoch {epoch} into the pack file for epoch {}",
                    self.epoch_meta.epoch
                ),
            ));
        }
        let declared = declared_batch_digests(header);
        let max_batches = max_batches_per_output(&self.epoch_meta.committee);
        if declared.len() > max_batches {
            return Err(PackError::TooManyBatches(max_batches));
        }
        if !declared.is_empty() {
            if let Some(foreign) = header
                .sub_dag
                .headers()
                .iter()
                .find(|h| self.epoch_meta.committee.authority(h.author()).is_none())
            {
                return Err(PackError::UnexpectedRecord(format!(
                    "certificate author {} is not in the epoch {} committee",
                    foreign.author(),
                    self.epoch_meta.epoch
                )));
            }
        }
        Ok((number - start, declared))
    }

    /// Append an imported output's header record and index its digest. The unused `extra` field
    /// is not part of the header digest; it is normalized to its default so the stored record is
    /// exactly what a locally-built output would store (`ConsensusOutput::consensus_header`).
    fn append_imported_header(
        &mut self,
        mut header: ConsensusHeader,
    ) -> Result<(u64, ConsensusHeaderDigest, u64), PackError> {
        header.extra = B256::default();
        let digest = header.digest();
        let number = header.number;
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(header)))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.consensus_digests
            .save(digest.into(), position)
            .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
        Ok((position, digest, number))
    }

    /// Append one imported batch record and index its digest.
    fn append_imported_batch(&mut self, digest: BlockHash, batch: Batch) -> Result<(), PackError> {
        let position = self
            .data
            .append(&PackRecord::Batch(batch))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.batch_digests
            .save(digest, position)
            .map_err(|e| PackError::IndexAppend(format!("batch {e}")))
    }

    /// Record a fully imported output in the position index and advance the digest indexes'
    /// data-length markers (the same bookkeeping as [`Self::append_output_records`]).
    fn finish_imported_output(
        &mut self,
        consensus_idx: u64,
        header_pos: u64,
    ) -> Result<(), PackError> {
        let len = self.data.file_len();
        self.consensus_pos_idx
            .save(consensus_idx, IndexPositions::new(header_pos, header_pos, len))
            .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
        self.consensus_digests.set_data_file_length(len);
        self.batch_digests.set_data_file_length(len);
        Ok(())
    }

    /// Write the batches for consensus to the pack file.
    fn save_consensus_batches(
        &mut self,
        batches: BTreeMap<BlockHash, Batch>,
    ) -> Result<(), PackError> {
        // Save all the required batches into the pack file.
        for (batch_digest, batch) in batches.into_iter() {
            let position = self
                .data
                .append(&PackRecord::Batch(batch))
                .map_err(|e| PackError::Append(e.to_string()))?;
            self.batch_digests
                .save(batch_digest, position)
                .map_err(|e| PackError::IndexAppend(format!("batch {e}")))?;
            let len = self.data.file_len();
            self.consensus_digests.set_data_file_length(len);
            self.batch_digests.set_data_file_length(len);
        }
        Ok(())
    }

    /// Save all the batches and consensus header from the ConsensusOutput the pack file.
    /// Returns the number of bytes the encoded ConsensusOutput takes in the pack file.
    ///
    /// Atomic: the append + index updates either all land, or the data log is rolled back to
    /// exactly its pre-save state, so a failed save never leaves an orphan record for an
    /// in-process retry to duplicate (a duplicate a later WAL rebuild would mis-sequence).
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
        // A consensus number below this epoch's first number can't index into this pack: reject it
        // rather than letting the `saturating_sub` above fold it onto index 0, where it would
        // either masquerade as an already-saved output or overwrite output 0. Symmetric
        // with the above-range check below.
        if consensus_number < self.epoch_meta.start_consensus_number {
            return Err(PackError::InvalidConsensusNumber(
                self.epoch_meta.start_consensus_number,
                consensus_number,
            ));
        }
        // Make sure this number is valid before we write anything...
        if (consensus_idx as usize) < self.consensus_pos_idx.len() {
            // Already saved: a no-op, which matters when consensus is replayed over outputs already
            // in the pack (e.g. after a restart). But only for the SAME output: a different output
            // under a stored number must not be reported as persisted (and then executed) while
            // the pack keeps the other one. We do need to return the bytes this output requires in
            // the pack file.
            let pos = self.consensus_pos_idx.load(consensus_idx)?;
            let stored = self.data.fetch(pos.consensus_header)?.into_consensus()?.digest();
            let got = consensus.consensus_header_hash();
            if stored != got {
                return Err(PackError::ConflictingOutput { number: consensus_number, stored, got });
            }
            return Ok(pos.output_end.saturating_sub(pos.output_start));
        } else if consensus_idx as usize != self.consensus_pos_idx.len() {
            return Err(PackError::InvalidConsensusNumber(
                self.consensus_pos_idx.len() as u64 + self.epoch_meta.start_consensus_number,
                consensus_number,
            ));
        }
        // The pack stores exactly the batches the output's sub-dag declares, and recovery replays
        // each output expecting exactly those batch records: an output carrying a partial or
        // extra batch set would be written fine but make the pack unrecoverable after the next
        // unclean shutdown. Refuse it before anything is written. An output carrying NO batches is
        // exempt: production only builds one for a sub-dag with no payload (the subscriber
        // fetches every declared batch otherwise), while test fixtures save committed sub-dags
        // whose batches do not exist through `ConsensusChain::write_subdag_for_test`.
        let batches = collect_batches(consensus);
        if !batches.is_empty() {
            let declared = sub_dag_batch_digests(consensus.sub_dag());
            if declared.iter().any(|digest| !batches.contains_key(digest)) {
                return Err(PackError::MissingBatches);
            }
            if batches.len() != declared.len() {
                return Err(PackError::ExtraBatches);
            }
        }
        // Snapshot the exact pre-append state so any mid-save error rolls back to it atomically.
        let data_start = self.data.file_len();
        let pos_idx_start = self.consensus_pos_idx.len();
        match self.append_output_records(consensus, consensus_idx, batches) {
            Ok(bytes) => Ok(bytes),
            Err(e) => {
                self.rollback_output(data_start, pos_idx_start);
                Err(e)
            }
        }
    }

    /// Append one output's records (header + batches) and index them. Split from
    /// [`Self::save_consensus_output`] so a mid-way error can be rolled back atomically by
    /// [`Self::rollback_output`]. The header-first layout writes the header, then its batches.
    fn append_output_records(
        &mut self,
        consensus: &ConsensusOutput,
        consensus_idx: u64,
        batches: BTreeMap<BlockHash, Batch>,
    ) -> Result<u64, PackError> {
        // Tests build genuine legacy (v0) packs to migrate; a release build never writes one.
        #[cfg(test)]
        if self.version() == 0 {
            return self.append_output_records_v0(consensus, consensus_idx, batches);
        }
        let consensus_digest = consensus.consensus_header_hash();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
        self.save_consensus_batches(batches)?;
        self.index_appended_output(consensus_digest, consensus_idx, position, position)
    }

    /// Test-only: [`Self::append_output_records`] in the legacy v0 (batches-first) layout, the
    /// output's batches before its header, so tests can build the packs a pre-v1 build left on
    /// disk for the migration to read.
    #[cfg(test)]
    fn append_output_records_v0(
        &mut self,
        consensus: &ConsensusOutput,
        consensus_idx: u64,
        batches: BTreeMap<BlockHash, Batch>,
    ) -> Result<u64, PackError> {
        let first_batch_pos = self.data.file_len();
        let has_batches = !batches.is_empty();
        self.save_consensus_batches(batches)?;
        let consensus_digest = consensus.consensus_header_hash();
        let position = self
            .data
            .append(&PackRecord::Consensus(Box::new(consensus.consensus_header())))
            .map_err(|e| PackError::Append(e.to_string()))?;
        let output_start = if has_batches { first_batch_pos } else { position };
        self.index_appended_output(consensus_digest, consensus_idx, position, output_start)
    }

    /// Index one appended output: its header digest, and its position entry (`position` of the
    /// header, `output_start` of its first record) ending at the current data length.
    fn index_appended_output(
        &mut self,
        consensus_digest: ConsensusHeaderDigest,
        consensus_idx: u64,
        position: u64,
        output_start: u64,
    ) -> Result<u64, PackError> {
        self.consensus_digests
            .save(consensus_digest.into(), position)
            .map_err(|e| PackError::IndexAppend(format!("consensus {e}")))?;
        let len = self.data.file_len();
        self.consensus_pos_idx
            .save(consensus_idx, IndexPositions::new(position, output_start, len))
            .map_err(|e| PackError::IndexAppend(format!("consensus number {e}")))?;
        self.consensus_digests.set_data_file_length(len);
        self.batch_digests.set_data_file_length(len);

        // Test-only: exercise the rollback at its worst case (everything appended and indexed).
        #[cfg(test)]
        if self.fail_save_after_append {
            self.fail_save_after_append = false;
            return Err(PackError::IndexAppend("injected mid-save failure".to_string()));
        }

        Ok(len.saturating_sub(output_start))
    }

    /// Roll the data log and position index back to the snapshot captured before a failed
    /// [`Self::append_output_records`], and mark the digest indexes for rebuild — making the save
    /// atomic.
    ///
    /// The data log's logical end is moved back with [`Pack::rewind_to`] (zeroing the abandoned
    /// region, no physical truncate/remap → no read-only-mmap SIGBUS window), so a retry or the
    /// next output appends exactly at `data_start`. The position index is rolled back to
    /// `pos_idx_start` (normally a no-op — index saves are atomic and there is no fallible step
    /// after the pos-index save today).
    ///
    /// The digest indexes are NOT surgically restored. A failed save may have overwritten a
    /// duplicate key in place — e.g. a batch digest already committed by an earlier output whose
    /// position is now clobbered to point into the discarded region. The `pos < file_len()`
    /// read mask cannot recover the earlier position, so rather than trust the digest indexes we
    /// invalidate their commit marker ([`HdxIndex::set_data_file_length`] to a value that can never
    /// equal the rewound data length): the next open fails [`Self::files_consistent`] and
    /// [`Self::recover_pack`] rebuilds every index from the data-log WAL, which now holds only the
    /// good pre-failure outputs. This is safe and complete because a failed output save is FATAL —
    /// the executor subscriber is a critical task, so the node shuts down and reopens the epoch via
    /// `open_append` (→ `recover_pack`) before the pack is served again. See the storage README
    /// ("Intentional design decisions").
    fn rollback_output(&mut self, data_start: u64, pos_idx_start: usize) {
        self.data.rewind_to(data_start);
        self.consensus_pos_idx.rewind_to_len(pos_idx_start);
        // 0 can never equal the real data length (always >= DATA_HEADER_BYTES), so
        // `files_consistent` always fails and the next open runs `recover_pack`, which rebuilds
        // every index from the (rewound) data-log WAL.
        const FORCE_INDEX_REBUILD: u64 = 0;
        self.consensus_digests.set_data_file_length(FORCE_INDEX_REBUILD);
        self.batch_digests.set_data_file_length(FORCE_INDEX_REBUILD);
    }

    /// True if consensus header by digest is found by digest.
    fn contains_consensus_header_number(&self, number: u64) -> bool {
        number >= self.epoch_meta.start_consensus_number
            && number < self.consensus_pos_idx.len() as u64 + self.epoch_meta.start_consensus_number
    }

    /// True if consensus header is found by digest.
    ///
    /// Delegates to [`Self::consensus_header_by_digest`] so membership carries the same guards as a
    /// real read: a position past the (possibly repaired) data end is masked, and the fetched
    /// record is re-hashed against `digest`. That way a stale index entry left by a rolled-back
    /// save — one that now points at an offset reused by a *different* record — can never report a
    /// spurious hit (which would wrongly suppress storing the real header).
    fn contains_consensus_header(&mut self, digest: ConsensusHeaderDigest) -> bool {
        self.consensus_header_by_digest(digest).is_some()
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
            //
            // Stamp the commit marker AFTER the data msync so it can never point past durable data
            // (fail-safe). It is a plain 16-byte mmap write in the capacity padding with NO extra
            // sync — best-effort, flushed by OS writeback / the next grow — so it adds nothing to
            // the persist hot path. `recover_pack` uses it to catch at-rest corruption
            // of the last committed output that the structural WAL probe cannot see.
            self.data.stamp_commit_marker();
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

    /// The latest stored consensus number. Unlike [`Self::latest_consensus_header`], this is
    /// defined even for a meta-only pack (no outputs): `start + len - 1` is `start - 1` there —
    /// the previous epoch's final consensus number. `start_consensus_number >= 1` (epoch 0 starts
    /// at 1) so this is always well-defined; the `saturating_sub` is defense-in-depth — a malformed
    /// meta with `start == 0` and no outputs fails safe to `0` (which the startup hint-clamp then
    /// treats as "no durable tip" and clamps down to) rather than wrapping to `u64::MAX` in release
    /// and silently defeating [`Self::clamp_latest_to_pack`].
    fn latest_consensus_number(&self) -> u64 {
        (self.epoch_meta.start_consensus_number + self.consensus_pos_idx.len() as u64)
            .saturating_sub(1)
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
    ///
    /// Delegates to [`Self::batch`] so membership carries the same guards as a real read (data-end
    /// masking + digest re-verification) and can never report a spurious hit from a stale index
    /// entry that points at an offset reused by a different record. A miss short-circuits in
    /// `load` before any fetch, so only a genuine hit pays the read.
    fn contains_batch(&mut self, digest: BlockHash) -> bool {
        self.batch(digest).is_some()
    }

    /// Return the Batch for digest if found.
    fn batch(&mut self, digest: BlockHash) -> Option<Batch> {
        let epoch = self.epoch_meta.epoch;
        let pos = self
            .batch_digests
            .load(digest)
            .inspect_err(|e| {
                if !fetch_error_is_absent(e) {
                    error!(target: "consensus_pack", epoch, ?digest, "batch digest index lookup failed (not a miss): {e}");
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

/// The dedup'd, sorted set of batch digests `header`'s sub-dag references. This is exactly the set
/// of batch records (and their order) the header-first layout stores after the header, matching
/// what [`collect_batches`] writes for a locally-saved output.
fn declared_batch_digests(header: &ConsensusHeader) -> BTreeSet<BlockHash> {
    sub_dag_batch_digests(&header.sub_dag)
}

/// The dedup'd, sorted set of batch digests `sub_dag`'s certificates reference.
fn sub_dag_batch_digests(sub_dag: &CommittedSubDag) -> BTreeSet<BlockHash> {
    sub_dag.headers().iter().flat_map(|cert_header| cert_header.payload().keys().copied()).collect()
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

/// Free space a stream import leaves on the filesystem it writes to. An import appends up to a
/// whole epoch of peer-supplied bytes before its only authentication (the final header against the
/// certified epoch record), and it shares that filesystem with the live pack and the node's other
/// stores, so it stops here rather than filling the disk under them.
pub const IMPORT_MIN_FREE_BYTES: u64 = 256 << 20;

/// How far an import's data log may grow between free-space checks.
const IMPORT_FREE_CHECK_EVERY: u64 = 64 << 20;

/// Stops a stream import before free space on `dir`'s filesystem drops below `min_free`. Falling
/// short is a local condition, not the sending peer's fault (an honest epoch on a nearly full disk
/// looks the same), so it is an [`io::ErrorKind::StorageFull`] I/O error that charges no penalty.
struct DiskFloor {
    dir: PathBuf,
    min_free: u64,
    /// The data length at the last check, `None` before the first.
    checked_at: Option<u64>,
}

impl DiskFloor {
    /// Check the free space once the import's data log has grown by [`IMPORT_FREE_CHECK_EVERY`]
    /// since the last check (and on the first call). An unknown free space does not stop the
    /// import: a real shortage still surfaces as a write error.
    fn check(&mut self, data_len: u64) -> Result<(), PackError> {
        if self.checked_at.is_some_and(|at| data_len < at.saturating_add(IMPORT_FREE_CHECK_EVERY)) {
            return Ok(());
        }
        self.checked_at = Some(data_len);
        match available_space(&self.dir) {
            Ok(free) if free < self.min_free => Err(PackError::IO(Arc::new(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "stream import into {} stopped: {free} bytes free, below the {} bytes an \
                     import must leave",
                    self.dir.display(),
                    self.min_free
                ),
            )))),
            _ => Ok(()),
        }
    }
}

/// Bytes available to an unprivileged writer on the filesystem holding `path`.
#[allow(clippy::unnecessary_cast)] // the `statvfs` field widths differ by platform
fn available_space(path: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt as _;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // SAFETY: `path` is a NUL-terminated string that outlives the call, and `stat` is writable
    // memory of the type `statvfs` fills in; it is read only after the call reported success (0),
    // which means every field was written.
    let stat = unsafe {
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        stat.assume_init()
    };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
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
///
/// Trust boundary: of the embedded committee, only its BLS key set is authenticated (against the
/// previous record's `next_committee`). Its other fields (each member's execution address,
/// network keys, host, stake) arrive unauthenticated from the peer, and a syncing node cannot
/// check them (it has not executed the previous epoch's final state). Today they reach only
/// telemetry (`CertifiedBatch::address`; fees go to each batch's own digest-covered
/// `beneficiary`), and nothing consensus-critical may come to depend on them.
///
/// [`Inner::open_append`] re-checks the same six fields, through
/// [`EpochMeta::authenticated_mismatch`], when it reopens a pack whose meta is already on disk,
/// but compares the key set against the committee its caller passes (the chain-derived one), not
/// against `previous_epoch.next_committee`, because `new_epoch` does not guarantee the two sets
/// are equal. It must never check more: a field this function does not authenticate holds the
/// serving peer's value in an imported pack, so comparing it would refuse a correctly imported
/// epoch.
/// Tightening starts here: authenticate a new field in this function first, then extend
/// `authenticated_mismatch` to match.
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
    let start_consensus_number = epoch_start_consensus_number(epoch, previous_epoch);
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

/// The first consensus number of `epoch`, given the record of the epoch before it.
///
/// Shared by [`Inner::open_append`], which writes it into a local meta, and [`verify_epoch_meta`],
/// which checks an imported meta against it, so the two can never disagree on where an epoch
/// starts.
fn epoch_start_consensus_number(epoch: Epoch, previous: &EpochRecord) -> u64 {
    if epoch == 0 {
        1
    } else {
        previous.final_consensus.number + 1
    }
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

// Test-only override for `output_buffer_budget` so the per-output decoded-memory bound can be
// exercised without GB-scale fixtures. Thread-local, so parallel tests don't interfere.
#[cfg(test)]
thread_local! {
    static TEST_OUTPUT_BUFFER_BUDGET: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Aggregate budget on the DECODED footprint (`tx_count * size_of::<Vec<u8>>() + tx_bytes`) of all
/// batches buffered for a single consensus output. `2x` the honest byte-content ceiling
/// (`max_batches_per_output * max_batch_size`): an honest output's per-transaction `Vec` overhead
/// is a small fraction of its bytes (real transactions are >= ~65 bytes), so this never rejects a
/// legitimate output, while a crafted flood of tiny transactions — each within the per-batch byte
/// cap but carrying 24 B of `Vec` overhead apiece — is rejected before it can exhaust memory.
fn output_buffer_budget(committee: &Committee) -> usize {
    #[cfg(test)]
    if let Some(limit) = TEST_OUTPUT_BUFFER_BUDGET.with(|c| c.get()) {
        return limit;
    }
    max_batches_per_output(committee)
        .saturating_mul(max_batch_size(committee.epoch()))
        .saturating_mul(2)
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
    // A sub-dag names its leader as its last header; an empty one has no leader, so every
    // `leader()`-derived accessor (`leader_epoch`, `nonce`, `commit_timestamp`, `Display`, ...)
    // would panic. A committed output always names a leader, so reject a peer-supplied empty
    // sub-dag here -- the decode chokepoint `iter_to_output` and the streamed import pass through
    // before any leader access -- rather than let it reach `save_consensus_output` and panic the
    // critical import task.
    if header.sub_dag.is_empty() {
        return Err(PackError::EmptySubDag);
    }
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

/// Decode raw pack-file `bytes` for one consensus output into a [`ConsensusOutput`], using the
/// pack's `compression` and `committee`. Only the header-first layout (v1, and v2 which differs
/// from it only in the data-file sentinel) is decoded: a legacy v0 pack is migrated to v2 before it
/// is ever read (`ConsensusChain::get_static`), so a v0 `version` is refused.
pub(crate) async fn decode_output_bytes(
    bytes: Vec<u8>,
    version: u16,
    compression: PackCompression,
    committee: &Committee,
) -> Result<ConsensusOutput, PackError> {
    check_header_first(version)?;
    let reader = BufReader::new(Cursor::new(bytes));
    bytes_to_output(reader, compression, Duration::from_secs(5), committee).await
}

/// The pack-record bytes served to peers for one consensus output: the raw header-first `bytes`
/// read out of the data file, as is. A legacy v0 pack is migrated before it is ever served, so a v0
/// `version` is refused.
pub(crate) fn serve_output_bytes(bytes: Vec<u8>, version: u16) -> Result<Vec<u8>, PackError> {
    check_header_first(version)?;
    Ok(bytes)
}

/// Refuse any pack data `version` other than the header-first layout (v1/v2).
fn check_header_first(version: u16) -> Result<(), PackError> {
    match version {
        1 | 2 => Ok(()),
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

/// Private helper to read the next record from a pack iterator or timeout if it takes
/// longer than timeout.
async fn next_output_record<R: AsyncRead + Unpin>(
    iter: &mut AsyncPackIter<PackRecord, R>,
    timeout: Duration,
) -> Result<Option<PackRecord>, PackError> {
    match tokio::time::timeout(timeout, iter.next()).await {
        Ok(Some(Ok(rec))) => Ok(Some(rec)),
        // Bytes that frame but fail their CRC, do not deserialize, or declare an oversized record
        // are a fault of whoever produced them. A transport error (including a truncated stream,
        // a throughput-floor cut or a peer abort) or a timeout says nothing about those bytes.
        Ok(Some(Err(
            e @ (FetchError::CrcFailed
            | FetchError::DeserializeValue(_)
            | FetchError::RequestedSizeTooLarge(..)
            | FetchError::RequestedDecompressSizeTooLarge(_)),
        ))) => Err(PackError::UndecodableRecord(e.to_string())),
        // A stream that ENDS inside a record was cut short by whoever produced it: a peer's sync
        // reader reports a clean end only after the peer's own `End` frame (a dropped connection
        // is `ConnectionAborted`), and a local file is simply truncated.
        Ok(Some(Err(FetchError::IO(e)))) if e.kind() == io::ErrorKind::UnexpectedEof => {
            Err(PackError::UndecodableRecord(format!("record truncated: {e}")))
        }
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
                return Err(PackError::UnexpectedRecord(
                    "unexpected epoch meta data found".to_string(),
                ))
            }
            PackRecord::Batch(_batch) => {
                return Err(PackError::UnexpectedRecord("unexpected batch found".to_string()))
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
    // any batches.  A legitimate ConsensusOutput references far fewer batches than this.
    let max_batches = max_batches_per_output(committee);
    if expected_digest_count > max_batches {
        return Err(PackError::TooManyBatches(max_batches));
    }
    let mut expected_batch_digests = expected_batch_digests.into_iter();

    let mut available_batches = HashMap::new();
    // Aggregate per-output budget on the DECODED footprint of the buffered batches (not just their
    // transaction bytes): each `Vec<u8>` transaction costs `size_of::<Vec<u8>>()` beyond its data,
    // so a flood of tiny transactions across the allowed batch fan-out could each pass the
    // per-batch byte cap yet still exhaust memory. 2x the honest byte-content ceiling — an
    // honest output's overhead is a small fraction of its bytes (real transactions are >= ~65
    // bytes) — so this never rejects a legitimate output while bounding the
    // crafted-tiny-transaction case.
    let output_buffer_limit = output_buffer_budget(committee);
    let mut buffered_decoded = 0usize;
    // Load and verify batches.  Batches are matched positionally against `expected_batch_digests`
    // (sorted digest order): producers write them in `BTreeMap`/`BTreeSet` digest order (see
    // `collect_batches` / `save_consensus_batches`), so the stream MUST arrive in that same order.
    // Out-of-order input is rejected below rather than silently reordered.
    let mut digest_count = 0;
    while let Some(record) = next_output_record(stream_iter, timeout).await? {
        match record {
            PackRecord::EpochMeta(_epoch_meta) => {
                return Err(PackError::UnexpectedRecord(
                    "unexpected epoch meta data found".to_string(),
                ))
            }
            PackRecord::Batch(batch) => {
                let Some(expected_digest) = expected_batch_digests.next() else {
                    return Err(PackError::UnexpectedRecord("unexpected batch found".to_string()));
                };
                let digest = batch.digest();
                if expected_digest != digest {
                    return Err(PackError::UnexpectedRecord(format!(
                        "unexpected batch found, expected {expected_digest}, got {}",
                        digest
                    )));
                }
                // Bound per-output buffering by the same measure the batch validator enforces at
                // production/gossip (`validate_batch_size_bytes`): the raw transaction-byte sum
                // against the epoch's `max_batch_size`. A record may be up to MAX_RECORD_SIZE
                // (16 MiB), but a legitimate batch is far smaller, so without this an attacker's
                // oversized batches inflate `available_batches` ~16x (finding #10 OOM). Rejecting
                // before the insert keeps only batches within the limit buffered.
                let batch_bytes = batch.transactions.iter().map(|tx| tx.len()).sum::<usize>();
                let max = max_batch_size(committee.epoch());
                if batch_bytes > max {
                    return Err(PackError::BatchTooLarge { size: batch_bytes, max });
                }
                // Charge this batch's decoded footprint against the per-output budget before
                // buffering.
                let batch_decoded = batch
                    .transactions
                    .len()
                    .saturating_mul(std::mem::size_of::<Vec<u8>>())
                    .saturating_add(batch_bytes);
                buffered_decoded = buffered_decoded.saturating_add(batch_decoded);
                if buffered_decoded > output_buffer_limit {
                    return Err(PackError::OutputTooLarge {
                        size: buffered_decoded,
                        max: output_buffer_limit,
                    });
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
                return Err(PackError::UnexpectedRecord(
                    "unexpected consensusheader found".to_string(),
                ))
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

/// Values stored in the position index.
/// Note for the header-first layout (v1/v2) consensus_header and output_start are the same value;
/// only a legacy v0 pack (read solely by its migration) differs. Dropping the field is an index
/// format change.
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

/// Errors returned by consensus pack operations.
#[derive(Debug, Clone)]
pub enum PackError {
    /// An underlying I/O error.
    IO(Arc<io::Error>),
    /// A required batch was not found.
    MissingBatch,
    /// Failed to load or decode a batch record.
    BatchLoad(String),
    /// Failed to load or decode the epoch meta record.
    EpochLoad(String),
    /// Failed to append a record to the data log.
    Append(String),
    /// Failed to append an entry to an index.
    IndexAppend(String),
    /// Failed to fetch a record from the data log.
    Fetch(String),
    /// Failed to open the pack's data file or one of its indexes.
    Open(Arc<OpenError>),
    /// The operation requires a writable pack but this one is read-only.
    ReadOnly,
    /// Expected a consensus-header record but found another kind.
    NotConsensus,
    /// Expected a batch record but found another kind.
    NotBatch,
    /// Expected the epoch-meta record but found another kind (or none).
    NotEpoch,
    /// A different output is already stored under this consensus number: saving `got` would be
    /// reported as persisted while the pack keeps `stored`.
    ConflictingOutput {
        /// The consensus number both outputs claim.
        number: u64,
        /// Digest of the output already in the pack.
        stored: ConsensusHeaderDigest,
        /// Digest of the output being saved.
        got: ConsensusHeaderDigest,
    },
    /// Error reading from a record stream: a transport failure or timeout, which says nothing
    /// about the bytes the sender produced.
    ReadError(String),
    /// A record in a consensus-output stream (a peer's epoch pack or requested output, or a pack
    /// being decoded) is out of place or is not what its header declares: an `EpochMeta` or header
    /// where a batch belongs, a batch before any header, or a batch whose digest is not the next
    /// one the header declares.
    UnexpectedRecord(String),
    /// A record in a consensus-output stream framed but failed its CRC, did not deserialize, or
    /// declared an oversized record.
    UndecodableRecord(String),
    /// A certificate author is not present in the pack's committee.
    MissingAuthority,
    /// The consensus headers do not form a valid parent-linked chain.
    InvalidConsensusChain,
    /// An output carried more batches than its sub-dag references.
    ExtraBatches,
    /// An output is missing batches that its sub-dag references.
    MissingBatches,
    /// The pack's epoch meta did not match what was expected for this epoch.
    InvalidEpoch(Epoch, String),
    /// Failed to send a request to the pack's background task.
    SendFailed,
    /// Failed to receive a response from the pack's background task.
    ReceiveFailed,
    /// Failed to durably persist the pack.
    PersistError(String),
    /// A consensus number did not match what the pack expected next, in the order `(expected,
    /// got)` — matching the `Display` impl and every construction site. (An out-of-range
    /// number the pack simply can't serve uses [`Self::ConsensusNumberTooLow`] /
    /// [`Self::ConsensusNumberTooHigh`].)
    InvalidConsensusNumber(u64, u64),
    /// The consensus output for this number was already written.
    ConsensusNumberAlreadyAdded,
    /// The pack holds damaged durably-committed data that recovery cannot repair by truncation.
    /// Carries an operator-facing message with the pack path and remediation guidance.
    CorruptPack(String),
    /// The requested consensus number is below this pack's range.
    ConsensusNumberTooLow,
    /// The requested consensus number is above this pack's range.
    ConsensusNumberTooHigh,
    /// A record stream declared more batches for one output than is allowed.
    TooManyBatches(usize),
    /// A data pack version this build does not read here (`.0` is the current version, `.1` the
    /// one found): newer than this build, or a legacy v0 pack outside its migration.
    InvalidVersion(u16, u16),
    /// A streamed consensus header's digest did not match the expected (already-verified) digest.
    /// Signals an unambiguous fork or peer misbehavior on the requested-output receive path.
    UnexpectedConsensusDigest {
        /// The digest that was expected (already verified out-of-band).
        expected: ConsensusHeaderDigest,
        /// The digest that was actually received in the stream.
        got: ConsensusHeaderDigest,
    },
    /// A decoded consensus header carried a sub-dag with no headers, and therefore no leader.
    /// Every `leader()`-derived accessor panics on such a value, so it is rejected at decode
    /// time; a legitimately committed output always names its leader as its last header.
    EmptySubDag,
    /// A `Batch` record in an import stream carried more transaction bytes than the epoch's
    /// `max_batch_size`. Legitimate batches are capped at production/gossip by the batch
    /// validator, so this is peer misbehavior; rejecting it bounds per-output buffering to a
    /// legitimate size.
    BatchTooLarge {
        /// The offending batch's transaction-byte total.
        size: usize,
        /// The per-epoch limit (`max_batch_size`) it exceeded.
        max: usize,
    },
    /// The batches buffered for one consensus output exceeded the per-output decoded-memory
    /// budget. Bounds the DECODED footprint (each `Vec<u8>` transaction costs
    /// `size_of::<Vec<u8>>()` beyond its bytes), so a flood of tiny transactions across the
    /// allowed batch fan-out cannot OOM the import even though each individual batch is within
    /// `max_batch_size`.
    OutputTooLarge {
        /// The decoded footprint accumulated so far.
        size: usize,
        /// The per-output budget it exceeded.
        max: usize,
    },
}

impl PackError {
    /// True when a static-pack open failed because the epoch's files are absent on disk: the
    /// data-file or an index-file open bottomed out in io `NotFound`. `Inner::open_static`
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

    /// True iff this is an index-open failure caused by the environment rather than by the index's
    /// contents: descriptor or memory exhaustion, a permission problem, an interrupted or
    /// would-block call, or a full disk/quota. Discarding the index cannot fix these and would
    /// throw away an index that may be intact (and the acked-output boundaries its position index
    /// holds), so the writable open surfaces them instead of rebuilding.
    pub fn is_environmental_index_error(&self) -> bool {
        let PackError::Open(open_error) = self else { return false };
        let OpenError::IndexFileOpen(LoadHeaderError::IO(io_error)) = open_error.as_ref() else {
            return false;
        };
        io_error_is_environmental(io_error)
    }

    /// True iff this is the "unwritten data file" open error: the `data` file has a physical size
    /// but is all zeros — a first write that sized the file (ftruncate + fsync) but crashed before
    /// the header was durable. There is nothing to rebuild; `repair_epoch` maps this to an
    /// actionable `Unrepairable`, and read-only doors surface it so the operator can remove the
    /// pack directory (a writable open reinitializes it in place).
    pub fn is_unwritten_data_file(&self) -> bool {
        matches!(
            self,
            PackError::Open(open_error)
                if matches!(
                    open_error.as_ref(),
                    OpenError::DataFileOpen(LoadHeaderError::Unwritten)
                )
        )
    }
}

/// True for an I/O failure caused by the environment rather than by a file's contents: descriptor
/// or memory exhaustion, a permission problem, an interrupted or would-block call, or a full
/// disk/quota. Discarding or condemning a file cannot fix these, and they clear on their own.
fn io_error_is_environmental(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::OutOfMemory
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::StorageFull
            | io::ErrorKind::QuotaExceeded
            | io::ErrorKind::ResourceBusy
    ) || matches!(error.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
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
            PackError::ConflictingOutput { number, stored, got } => write!(
                f,
                "a different output is already stored for consensus number {number} (stored \
                 {stored}, saving {got})"
            ),
            PackError::UnexpectedRecord(error) => write!(f, "Unexpected record ({error})"),
            PackError::UndecodableRecord(error) => write!(f, "Undecodable record ({error})"),
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
                write!(f, "Unsupported pack file version {got} (current version {expected})")
            }
            PackError::UnexpectedConsensusDigest { expected, got } => {
                write!(f, "Consensus header digest mismatch: expected {expected}, got {got}")
            }
            PackError::EmptySubDag => {
                write!(f, "consensus header carries an empty sub-dag (no leader)")
            }
            PackError::BatchTooLarge { size, max } => {
                write!(f, "batch of {size} transaction bytes exceeds the {max}-byte limit")
            }
            PackError::OutputTooLarge { size, max } => {
                write!(
                    f,
                    "buffered batches for one output decode to {size} bytes, exceeds the \
                     {max}-byte per-output budget"
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
        path::Path,
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
        consensus_pack::{
            epoch_start_consensus_number, max_batches_per_output, ConsensusPack, EpochMeta,
            EpochMigrate, EpochRepair, Inner, PackRecord, PACK_VERSION,
        },
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
            builder = builder.with_payload_batch(batch, 0_u16);
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
    async fn test_pack_save_below_start_number_rejected() {
        let temp_dir = TempDir::with_prefix("test_pack_below_start").expect("temp dir");
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

        // Epoch 0's first consensus number is `start_consensus_number` (1). A correct-epoch output
        // whose number is below that must be rejected, not folded onto index 0 by `saturating_sub`
        // (where it would masquerade as already-saved or overwrite output 0).
        let parent = ConsensusHeader::default().digest();
        let below = make_test_output(&committee, 0, chain.clone(), 0, parent);
        assert_eq!(below.sub_dag().leader_epoch(), committee.epoch());
        let err = pack.save_consensus_output(below).await;

        assert!(
            matches!(err, Err(super::PackError::InvalidConsensusNumber(1, 0))),
            "expected InvalidConsensusNumber(1, 0), got {err:?}"
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
            parent = consensus_output.digest();
            outputs.push(consensus_output.clone());
            pack.save_consensus_output(consensus_output).await.unwrap();
        }
        for i in 0..num_outputs {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .unwrap_or_else(|_| panic!("consensus output for {}", i + 1));
            let output = outputs.get(i).unwrap();
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
            parent = consensus_output.digest();
            outputs.push(consensus_output.clone());
            pack.save_consensus_output(consensus_output).await.unwrap();
        }
        for i in 0..(num_outputs * 2) {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .unwrap_or_else(|e| panic!("failed output on {i}: {e}"));
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }
        pack.persist().await.expect("persist");
        drop(pack);

        // Open read only and verify.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).unwrap();
        for i in 0..(num_outputs * 2) {
            let output_db = pack.get_consensus_output(i as u64 + 1).await.unwrap();
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }
        assert!(pack.get_consensus_output(num_outputs as u64 * 2).await.is_ok());
        drop(pack);

        // Make sure we can stream the file to create another pack file. Production bounds the
        // export to the logical length (`data_file_len()`), so the clean-close sentinel is
        // never streamed; mirror that here by capping the raw-file stream at the logical
        // length.
        {
            use tokio::io::AsyncReadExt as _;
            let temp_dir2 = TempDir::with_prefix("test_consensus_pack").expect("temp dir");
            let data_file = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
            let logical_len = std::fs::metadata(&data_file).expect("meta").len()
                - crate::archive::data_file::SENTINEL_LEN;
            let stream =
                tokio::fs::File::open(&data_file).await.expect("log file").take(logical_len);
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
                let output = outputs.get(i).unwrap();
                compare_outputs(&output_db, output);
            }
            for i in 0..num_outputs {
                let output_db =
                    pack.get_consensus_output((i + num_outputs) as u64 + 1).await.unwrap();
                let output = outputs.get(i + num_outputs).unwrap();
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
        // Strip the 8-byte clean-close sentinel and one more byte so the truncation damages the
        // last record (not just the sentinel).
        stream.set_len(stream_len - crate::archive::data_file::SENTINEL_LEN - 1).unwrap();
        drop(stream);
        // Reopen in append and load some more data.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        for i in 0..(num_outputs * 2) - 1 {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .unwrap_or_else(|_| panic!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }
        assert!(pack.get_consensus_output(num_outputs as u64 * 2).await.is_err());
        let last_output = outputs.last().unwrap().clone();
        pack.save_consensus_output(last_output).await.unwrap();

        for i in 0..(num_outputs * 2) - 1 {
            let output_db = pack
                .get_consensus_output(i as u64 + 1)
                .await
                .unwrap_or_else(|_| panic!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i).unwrap();
            compare_outputs(&output_db, output);
        }

        let output_db = pack.get_consensus_output(num_outputs as u64 * 2).await.unwrap();
        let output = outputs.get((num_outputs * 2) - 1).unwrap();
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
                .unwrap_or_else(|_| panic!("failed to get output (damage 1) {i}"));
            let output = outputs.get(i).unwrap();
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
        let output_2 = make_test_output(&committee, 2, chain.clone(), 2, output_1.digest());
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

        // Stream into a new pack (peer epoch sync path) and read back. Bound to the logical length
        // (as production's export does) so the clean-close sentinel is not streamed.
        use tokio::io::AsyncReadExt as _;
        let temp_dir2 = TempDir::with_prefix("test_consensus_pack_dup2").expect("temp dir");
        let data_file =
            temp_dir.path().join(format!("epoch-{SHARED_BATCH_EPOCH}")).join(Inner::DATA_NAME);
        let logical_len = std::fs::metadata(&data_file).expect("meta").len()
            - crate::archive::data_file::SENTINEL_LEN;
        let stream = tokio::fs::File::open(&data_file).await.expect("log file").take(logical_len);
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
        assert!(matches!(res, Err(PackError::UnexpectedRecord(_))), "expected UnexpectedRecord");
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

    /// Finding #10 (OOM): the batch buffer in `iter_to_output` is bounded only by count, not bytes,
    /// so a `Batch` whose transaction bytes exceed `max_batch_size(epoch)` must be rejected — the
    /// same limit the batch validator enforces at production/gossip — rather than buffered at up to
    /// `MAX_RECORD_SIZE` (16 MiB) each. Without the cap this decodes `Ok`; with it,
    /// `BatchTooLarge`.
    #[tokio::test]
    async fn test_iter_to_output_rejects_oversized_batch() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_output, PackError, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_oversized_batch").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // One batch whose single transaction is one byte over the epoch's limit; well under the
        // 16 MiB record cap, so it decodes and reaches the byte check rather than being rejected as
        // an oversized record.
        let oversized = Batch::new_for_test(
            vec![vec![0_u8; tn_types::max_batch_size(committee.epoch()) + 1]],
            ExecHeader::default(),
            0,
            committee.epoch(),
        );

        // A leader header that references exactly that batch (so the output declares one batch and
        // the decoder reaches the buffering loop).
        let authority = committee.authorities();
        let authority = authority.first().expect("committee has authorities");
        let mut leader = Certificate::default();
        leader.update_header_author_for_test(authority.id());
        leader.update_header_for_test(
            HeaderBuilder::from_header(leader.header())
                .with_payload_batch(&oversized, 0_u16)
                .build(),
        );
        leader.update_header_epoch_for_test(committee.epoch());
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let header = ConsensusHeader {
            parent_hash: Default::default(),
            sub_dag,
            number: 1,
            extra: Default::default(),
        };

        // v1 stream: header first, then the oversized batch record.
        let path = temp_dir.path().join("oversized");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            pack.append(&PackRecord::Consensus(Box::new(header))).expect("append header");
            pack.append(&PackRecord::Batch(oversized)).expect("append oversized batch");
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
        assert!(
            matches!(res, Err(PackError::BatchTooLarge { .. })),
            "an oversized batch must be rejected as BatchTooLarge, got {res:?}"
        );
    }

    /// A batch carrying a zero-byte (empty) transaction is invalid and must fail the import at
    /// decode — a peer uses a flood of empty transactions (which compress to almost nothing but
    /// decode to a huge `Vec<Vec<u8>>`) to exhaust memory. The `Batch` deserializer rejects it, so
    /// `bytes_to_output` errors instead of buffering it.
    #[tokio::test]
    async fn test_iter_to_output_rejects_empty_transaction() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_output, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_empty_tx").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // One batch whose single transaction is zero bytes.
        let bad = Batch::new_for_test(vec![vec![]], ExecHeader::default(), 0, committee.epoch());
        let authority = committee.authorities();
        let authority = authority.first().expect("committee has authorities");
        let mut leader = Certificate::default();
        leader.update_header_author_for_test(authority.id());
        leader.update_header_for_test(
            HeaderBuilder::from_header(leader.header()).with_payload_batch(&bad, 0_u16).build(),
        );
        leader.update_header_epoch_for_test(committee.epoch());
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let header = ConsensusHeader {
            parent_hash: Default::default(),
            sub_dag,
            number: 1,
            extra: Default::default(),
        };

        let path = temp_dir.path().join("empty_tx");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            pack.append(&PackRecord::Consensus(Box::new(header))).expect("append header");
            // Encoding does not validate, so the malicious batch is written; decode must reject it.
            pack.append(&PackRecord::Batch(bad)).expect("append empty-tx batch");
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
        assert!(
            res.is_err(),
            "a batch with a zero-byte transaction must be rejected on import, got {res:?}"
        );
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

    /// Adiri replay pin: at epochs at or below `ADIRI_DUP_BATCH_EPOCH` the rebuild must DROP a
    /// shared batch from the second certificate, reproducing the historical duplicate-batch
    /// outputs so adiri testnet can sync (the gate in `iter_to_output`). Adiri's oldest epochs are
    /// v0 packs, read only after their migration to v2, so this builds one, migrates it, and checks
    /// the skip side on both the local read and the decode of the served bytes. The push side
    /// above the cutoff is exercised by the two shared-batch tests at [`SHARED_BATCH_EPOCH`]
    /// (#1128).
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

        // A v0 pack, as a pre-v1 build left it on disk.
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
        pack.close().await;
        strip_sentinel(&temp_dir.path().join("epoch-0").join(Inner::DATA_NAME));
        match ConsensusPack::migrate_epoch(temp_dir.path(), 0, true).await.expect("migrate") {
            EpochMigrate::Migrated(_) => {}
            other => panic!("expected the v0 pack to migrate, got {other:?}"),
        }
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open migrated pack");

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

        // Local read of the migrated pack.
        assert_replay_shape(&pack.get_consensus_output(1).await.expect("local read"));

        // The same output's served bytes, decoded as a peer decodes them.
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
        pack.close().await;
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
            parent = output.digest();
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
                parent = output.digest();
                pack.save_consensus_output(output).await.unwrap();
            }
            pack.persist().await.expect("persist");
        }

        // Damage the tail of the data file (truncate last byte of the last record).
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            let len = f.metadata().expect("meta").len();
            // Strip the clean-close sentinel and one more byte so the last record is truly damaged.
            f.set_len(len - crate::archive::data_file::SENTINEL_LEN - 1).expect("truncate");
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
        build_test_pack_version(temp_dir, committee, chain, previous_epoch, n, PACK_VERSION).await;
    }

    /// Build `n` sequential outputs into a fresh pack stamped with an explicit data-file `version`
    /// and persist the data log. v0/v1 exercise the pre-sentinel legacy formats a pre-mmap build
    /// wrote; the current `PACK_VERSION` (v2) is the sentinel-era format.
    async fn build_test_pack_version(
        temp_dir: &TempDir,
        committee: &Committee,
        chain: &Arc<RethChainSpec>,
        previous_epoch: &EpochRecord,
        n: u64,
        version: u16,
    ) -> Vec<ConsensusOutput> {
        let pack = ConsensusPack::open_append_version(
            temp_dir.path(),
            previous_epoch.clone(),
            committee.clone(),
            version,
        )
        .expect("open pack");
        let mut parent = ConsensusHeader::default().digest();
        let mut outputs = Vec::new();
        for i in 0..n {
            let output =
                make_test_output(committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.expect("save output");
        }
        pack.persist().await.expect("persist");
        outputs
    }

    /// For a current-version (v2) pack the clean-close sentinel is the *definitive* consistency
    /// gate: a pack whose lengths all still agree (physical == logical == index markers) but which
    /// lost its data-file sentinel is treated as inconsistent. `open_static` refuses it
    /// (CorruptPack); `open_append_exists` recovers it. (A pre-sentinel v0/v1 pack has no sentinel
    /// to lose — see `test_open_static_accepts_sentinelless_legacy_pack`.)
    #[tokio::test]
    async fn test_missing_sentinel_forces_recovery_even_when_lengths_agree() {
        let temp_dir = TempDir::with_prefix("test_missing_sentinel").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Strip exactly the 8-byte clean-close sentinel from the data file. The physical length is
        // now the logical length, which equals the index markers, so the length-based checks still
        // pass — only the missing sentinel signals that the file was not cleanly closed.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let f = OpenOptions::new().write(true).open(&data_path).expect("open data");
            let len = f.metadata().expect("meta").len();
            f.set_len(len - crate::archive::data_file::SENTINEL_LEN).expect("strip sentinel");
        }

        // Read-only door: a pack that was not cleanly sealed is rejected rather than served.
        match ConsensusPack::open_static(temp_dir.path(), 0) {
            Err(super::PackError::CorruptPack(_)) => {}
            other => panic!("open_static must reject an unsealed pack, got {other:?}"),
        }

        // Write door: the same unsealed pack recovers (rebuilds from the WAL) and reads back.
        let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
            .expect("open_append_exists must recover an unsealed pack");
        for i in 1..=3 {
            assert!(
                pack.get_consensus_output(i).await.is_ok(),
                "output {i} must read back after recovery"
            );
        }
    }

    /// Migration (finding #3): a pack written before the clean-close sentinel existed (v0/v1) has
    /// no sentinel on disk, so the pre-sentinel version must be recognized and the missing
    /// sentinel treated as normal rather than as an unclean shutdown. `open_static` must open such
    /// a pack instead of returning `CorruptPack` — `ConsensusChain::get_static` opens it to find it
    /// is legacy and migrate it before anything reads it — and must not rewrite the file. The
    /// cross-file length checks still carry the integrity guarantee (exactly the length-only test
    /// pre-mmap `main` used). A v1 pack's outputs decode like v2's; a v0 pack's are never decoded
    /// (only its migration reads it), so its reads are refused.
    #[tokio::test]
    async fn test_open_static_accepts_sentinelless_legacy_pack() {
        for version in [0_u16, 1] {
            let temp_dir = TempDir::with_prefix("test_legacy_static").expect("temp dir");
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let committee = fixture.committee();
            let previous_epoch = test_previous_epoch(&committee);
            build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, version)
                .await;

            // Synthesize the pre-PR on-disk shape: strip the 8-byte clean-close sentinel the
            // current build appends on close, leaving a bare v{0,1} data file just as
            // the buffered backend wrote it (physical == logical, no sentinel).
            let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
            let len_before = {
                let f = OpenOptions::new().write(true).open(&data_path).expect("open data");
                let len = f.metadata().expect("meta").len();
                let stripped = len - crate::archive::data_file::SENTINEL_LEN;
                f.set_len(stripped).expect("strip sentinel");
                stripped
            };

            // The read-only door must accept it. Before the version gate this returned CorruptPack
            // for every pre-upgrade epoch, halting peer sync and restart-time state restore.
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).unwrap_or_else(|e| {
                panic!("open_static must accept a sentinel-less v{version} pack, got {e:?}")
            });
            assert!(pack.is_legacy(), "v{version} must be recognized as legacy");
            for i in 1..=3 {
                let read = pack.get_consensus_output(i).await;
                if version == 0 {
                    assert!(
                        matches!(read, Err(super::PackError::InvalidVersion(_, 0))),
                        "a v0 output is never decoded, got {read:?}"
                    );
                } else {
                    assert!(read.is_ok(), "v1 output {i} must read back, got {read:?}");
                }
            }
            pack.close().await;

            // A read-only open must not have re-sealed or otherwise rewritten the data file.
            let len_after = std::fs::metadata(&data_path).expect("meta").len();
            assert_eq!(len_after, len_before, "open_static must not mutate a v{version} pack");
        }
    }

    /// Peek the on-disk data-file format version of an epoch-0 pack (read-only, no mutation).
    fn peek_pack_version(data_path: &Path) -> u16 {
        Pack::<PackRecord>::open(data_path, 0, true, PackCompression::ZStd, PACK_VERSION)
            .expect("open data read-only")
            .version()
    }

    /// Strip the 8-byte clean-close sentinel the current build appends on close, synthesizing the
    /// genuine pre-PR on-disk shape of a v0/v1 pack (physical == logical, no sentinel).
    fn strip_sentinel(data_path: &Path) -> u64 {
        let f = OpenOptions::new().write(true).open(data_path).expect("open data");
        let len = f.metadata().expect("meta").len();
        let stripped = len - crate::archive::data_file::SENTINEL_LEN;
        f.set_len(stripped).expect("strip sentinel");
        stripped
    }

    /// HIGH-1: a sealed legacy (pre-v2) pack's on-disk digest index was written under the OLD key
    /// placement, so a by-digest lookup would silently miss present records. `build_static_heal`
    /// — the path `get_static` now routes a legacy pack through on read — must ACCEPT the
    /// legacy pack (previously rejected: a legacy pack has no sentinel so it reads back
    /// `opened_unclean == true`, which the old seal-only guard rejected), migrate it to v2, and
    /// rebuild the indexes under the current placement, so by-digest AND by-number both resolve
    /// afterward.
    ///
    /// (This build can only write the current `stable_hash` placement, so the fixture cannot
    /// reproduce the exact old-placement bytes; the test instead pins the mechanism that closes the
    /// gap — heal accepts a legacy pack, migrates it to v2, and every header resolves by digest
    /// after migration. Pre-fix this test fails at the heal call, which
    /// returned `CorruptPack` for a legacy pack.)
    #[tokio::test]
    async fn test_heal_static_indexes_migrates_legacy_pack_by_digest() {
        for version in [0_u16, 1] {
            let temp_dir = TempDir::with_prefix("heal_legacy_by_digest").expect("temp dir");
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let committee = fixture.committee();
            let previous_epoch = test_previous_epoch(&committee);
            build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, version)
                .await;
            let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
            strip_sentinel(&data_path);
            assert_eq!(peek_pack_version(&data_path), version, "precondition: on-disk v{version}");
            assert!(
                ConsensusPack::epoch_is_legacy(temp_dir.path(), 0),
                "v{version}: pack must be detected as legacy before healing"
            );

            // Capture each header's digest (by number, the placement-independent path).
            let before =
                ConsensusPack::open_static(temp_dir.path(), 0).expect("open legacy static");
            let mut digests = Vec::new();
            for i in 1..=3u64 {
                digests
                    .push(before.consensus_header_by_number(i).await.expect("legacy hdr").digest());
            }
            before.close().await;

            // Read-side heal: a legacy pack must be accepted, migrated to v2, and its indexes
            // rebuilt.
            let heal = ConsensusPack::build_static_heal(temp_dir.path(), 0)
                .unwrap_or_else(|e| {
                    panic!("v{version}: heal must migrate a legacy pack, got {e:?}")
                })
                .expect("a legacy pack always needs a heal");
            ConsensusPack::install_static_heal(temp_dir.path(), 0, heal)
                .unwrap_or_else(|e| panic!("v{version}: installing the migration failed: {e:?}"));
            assert_eq!(peek_pack_version(&data_path), PACK_VERSION, "v{version}: migrated to v2");
            assert!(
                !ConsensusPack::epoch_is_legacy(temp_dir.path(), 0),
                "v{version}: pack must no longer be legacy after migration"
            );

            // Every header now resolves by digest (placement-dependent) AND by number.
            let after =
                ConsensusPack::open_static(temp_dir.path(), 0).expect("open migrated static");
            for (idx, digest) in digests.iter().enumerate() {
                let n = idx as u64 + 1;
                assert!(
                    after.contains_consensus_header(*digest).await,
                    "v{version}: header {n} missing by digest after heal"
                );
                let by_digest =
                    after.consensus_header_by_digest(*digest).await.unwrap_or_else(|| {
                        panic!("v{version}: header {n} not found by digest after heal")
                    });
                assert_eq!(by_digest.digest(), *digest, "v{version}: wrong header by digest");
                assert!(
                    after.get_consensus_output(n).await.is_ok(),
                    "v{version}: output {n} must read back by number"
                );
            }
            after.close().await;
        }
    }

    /// F6: a legacy v0 (batches-first) or v1 (header-first) pack migrates 1:1 into a current v2
    /// pack. After migration the pack is v2, opens through the sentinel-gated read-only door,
    /// and every consensus output round-trips unchanged. Re-running the migration is a no-op
    /// (`AlreadyCurrent`).
    #[tokio::test]
    async fn test_migrate_legacy_pack_roundtrip() {
        for version in [0_u16, 1] {
            let temp_dir = TempDir::with_prefix("test_migrate_roundtrip").expect("temp dir");
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let committee = fixture.committee();
            let previous_epoch = test_previous_epoch(&committee);
            // The original outputs, for a 1:1 comparison after migration.
            let expected =
                build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, version)
                    .await;
            let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
            strip_sentinel(&data_path);
            assert_eq!(peek_pack_version(&data_path), version, "precondition: on-disk v{version}");

            match ConsensusPack::migrate_epoch(temp_dir.path(), 0, true).await.expect("migrate") {
                EpochMigrate::Migrated(_) => {}
                other => panic!("v{version}: expected Migrated, got {other:?}"),
            }

            // Now v2, opens read-only clean, and every output matches the pre-migration bytes.
            assert_eq!(peek_pack_version(&data_path), PACK_VERSION, "v{version} -> v2");
            let after = ConsensusPack::open_static(temp_dir.path(), 0).unwrap_or_else(|e| {
                panic!("v{version}: migrated pack must open_static, got {e:?}")
            });
            for (i, want) in expected.iter().enumerate() {
                let got =
                    after.get_consensus_output(i as u64 + 1).await.expect("read migrated output");
                assert_eq!(
                    got.digest(),
                    want.digest(),
                    "v{version}: output {} must survive migration unchanged",
                    i + 1
                );
            }
            after.close().await;

            // Idempotent: a second migration finds it already current and writes nothing.
            match ConsensusPack::migrate_epoch(temp_dir.path(), 0, true).await.expect("migrate 2") {
                EpochMigrate::AlreadyCurrent => {}
                other => panic!("v{version}: re-migrate expected AlreadyCurrent, got {other:?}"),
            }
            // No temp/aside dirs are left behind.
            assert!(!temp_dir.path().join("epoch-0.migrating").exists());
            assert!(!temp_dir.path().join("epoch-0.replaced").exists());
        }
    }

    /// Migration replaces the whole `epoch-N` directory, so it must carry over the epoch's
    /// per-epoch certificate pack (`cert_data` + `cert_hash/`). Otherwise upgrading a node
    /// mid-epoch (which migrates the in-progress epoch on its writable reopen) drops that
    /// epoch's certificate archive.
    #[tokio::test]
    async fn test_migrate_keeps_certificate_pack() {
        let temp_dir = TempDir::with_prefix("test_migrate_certs").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let epoch_dir = temp_dir.path().join("epoch-0");
        strip_sentinel(&epoch_dir.join(Inner::DATA_NAME));
        let cert_data = epoch_dir.join(crate::certificate_pack::DATA_NAME);
        let cert_index = epoch_dir.join(crate::certificate_pack::HASH_NAME).join("index.hdx");
        std::fs::write(&cert_data, b"cert log bytes").expect("write cert data");
        std::fs::create_dir_all(cert_index.parent().expect("parent")).expect("cert index dir");
        std::fs::write(&cert_index, b"cert index bytes").expect("write cert index");

        let outcome =
            ConsensusPack::migrate_epoch(temp_dir.path(), 0, true).await.expect("migrate");
        assert!(matches!(outcome, EpochMigrate::Migrated(_)), "got {outcome:?}");
        assert_eq!(peek_pack_version(&epoch_dir.join(Inner::DATA_NAME)), PACK_VERSION);
        assert_eq!(std::fs::read(&cert_data).expect("cert data kept"), b"cert log bytes");
        assert_eq!(std::fs::read(&cert_index).expect("cert index kept"), b"cert index bytes");
    }

    /// F6: a migration dry run (`apply == false`) reports what it would do but writes nothing — the
    /// on-disk version and length are unchanged and no temp dir is left behind.
    #[tokio::test]
    async fn test_migrate_dry_run_writes_nothing() {
        let temp_dir = TempDir::with_prefix("test_migrate_dry").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let len_before = strip_sentinel(&data_path);

        match ConsensusPack::migrate_epoch(temp_dir.path(), 0, false).await.expect("dry run") {
            EpochMigrate::WouldMigrate(_) => {}
            other => panic!("expected WouldMigrate, got {other:?}"),
        }
        assert_eq!(peek_pack_version(&data_path), 1, "dry run must not change the version");
        assert_eq!(
            std::fs::metadata(&data_path).expect("meta").len(),
            len_before,
            "dry run must not change the data file"
        );
        assert!(!temp_dir.path().join("epoch-0.migrating").exists());
    }

    /// F6 regression (the core bug): a legacy pack whose committed final record is damaged must NOT
    /// be silently truncated. `db repair --force` (via `repair_epoch`) on such a v1 pack reports it
    /// Unrepairable and leaves the pack byte-for-byte untouched (still v1, same length, no
    /// temp/aside dirs) — never the old "REPAIRED" that dropped the committed output.
    #[tokio::test]
    async fn test_repair_legacy_damaged_tail_is_not_truncated() {
        let temp_dir = TempDir::with_prefix("test_migrate_f6").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let len = strip_sentinel(&data_path);

        // Corrupt the final committed record by flipping the last byte (part of its CRC). The pack
        // is length-consistent (a complete, sealed-equivalent legacy pack), so this is
        // at-rest damage, not a truncatable unacked tail.
        let mut bytes = std::fs::read(&data_path).expect("read data");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&data_path, &bytes).expect("write corrupted data");

        match ConsensusPack::repair_epoch(temp_dir.path(), 0, true).await.expect("repair") {
            EpochRepair::Unrepairable(_) => {}
            other => panic!("F6: a damaged legacy tail must be Unrepairable, got {other:?}"),
        }
        // Untouched: still v1 and the same length (no truncation, no partial migration installed).
        assert_eq!(peek_pack_version(&data_path), 1, "must not migrate a corrupt legacy pack");
        assert_eq!(
            std::fs::metadata(&data_path).expect("meta").len(),
            len,
            "F6: the committed data must not be truncated"
        );
        assert!(!temp_dir.path().join("epoch-0.migrating").exists());
        assert!(!temp_dir.path().join("epoch-0.replaced").exists());
    }

    /// A node reopening its current epoch for append after an upgrade migrates the legacy pack to
    /// v2 transparently through `open_append_exists`, then keeps appending: the pack becomes
    /// v2, the pre-upgrade outputs still read back, and a freshly appended output reads back
    /// too.
    #[tokio::test]
    async fn test_open_append_auto_migrates_legacy() {
        let temp_dir = TempDir::with_prefix("test_migrate_open_append").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        strip_sentinel(&data_path);

        // Reopen the (legacy) current epoch for append — this migrates it up to v2 in place.
        let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
            .expect("open_append_exists must migrate and open a legacy pack");
        assert_eq!(peek_pack_version(&data_path), PACK_VERSION, "reopened pack must be v2");

        // Continue writing: append a 4th output chained off output 3.
        let parent = pack.get_consensus_output(3).await.expect("read output 3").digest();
        let output = make_test_output(&committee, 3, chain.clone(), 4, parent);
        pack.save_consensus_output(output).await.expect("append after migration");
        pack.persist().await.expect("persist");
        pack.close().await;

        // All four outputs read back through the sentinel-gated read-only door.
        let ro =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open migrated+appended pack");
        for i in 1..=4 {
            assert!(ro.get_consensus_output(i).await.is_ok(), "output {i} must read back");
        }
        ro.close().await;
    }

    /// `validate_pack_file` cross-checks every derived index entry against the data log, so a
    /// zeroed BLOOM filter (which the bucket-CRC scan cannot see — the buckets are still valid) now
    /// flips the verdict to `Invalid` via an `IndexMismatch`, and `db repair` rebuilds it back to
    /// `Valid`/`Healthy`. Before the cross-check this reported `Valid`+`Healthy` while every lookup
    /// silently missed.
    #[tokio::test]
    async fn test_validate_cross_check_catches_zeroed_bloom() {
        use crate::pack_validate::{validate_pack_file, PackIssue, Verdict};

        let temp_dir = TempDir::with_prefix("test_xcheck_bloom").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // Control: a clean pack validates and repairs as Healthy.
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Valid,
            "a clean pack must validate"
        );

        // Zero ONLY the bloom region of the consensus (hash) index — the buckets after it stay CRC
        // valid, so the bucket scan still passes; only the cross-check can catch this.
        let hdx = temp_dir.path().join("epoch-0").join("hash").join("index.hdx");
        {
            // hdx layout: [68-byte header][BLOOM_SIZE_BYTES bloom][buckets...].
            const HDX_HEADER: usize = 68;
            let bloom = crate::archive::digest_index::bloom::BLOOM_SIZE_BYTES;
            let mut bytes = std::fs::read(&hdx).expect("read hdx");
            for b in &mut bytes[HDX_HEADER..HDX_HEADER + bloom] {
                *b = 0;
            }
            std::fs::write(&hdx, &bytes).expect("write hdx");
        }

        let report = validate_pack_file(&data_path, 0, None).expect("validate");
        assert_eq!(report.verdict, Verdict::Invalid, "zeroed bloom must be Invalid: {report}");
        assert!(
            report.issues.iter().any(|i| matches!(i, PackIssue::IndexMismatch { .. })),
            "must be flagged as an index/log mismatch: {report}"
        );
        assert!(
            report.index_scan.is_none_or(|s| s.is_clean()),
            "the bucket scan must be clean — this is a bloom (cross-check) miss, not a bucket defect"
        );

        // `db repair` rebuilds the index (bloom included) and it validates clean again.
        match ConsensusPack::repair_epoch(temp_dir.path(), 0, true).await.expect("repair") {
            EpochRepair::Repaired(_) => {}
            other => panic!("expected Repaired, got {other:?}"),
        }
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("re-validate").verdict,
            Verdict::Valid,
            "repair must rebuild the bloom back to Valid"
        );
    }

    /// A tear INSIDE the in-flight output (header + some batches durable, the last batch torn
    /// — the ordinary crash shape) must not report the unwritten batches as false "absent".
    /// Bounding the prefix walk at the WAL `consistent_end` (last COMPLETE output) is Valid;
    /// the old bound at the first-bad-record offset falsely reported INVALID with absent
    /// batches.
    #[tokio::test]
    async fn test_validate_bounded_at_consistent_end_no_false_absent() {
        use crate::pack_validate::{
            classify_physical_corruption, validate_pack_file_bounded, BatchClass, Verdict,
        };

        let temp_dir = TempDir::with_prefix("test_tear_in_output").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // Make it an unclean pack with a tear inside the final output: strip the sentinel, then lop
        // one byte off the last batch record (its CRC read now falls short).
        strip_sentinel(&data_path);
        {
            let f = OpenOptions::new().write(true).open(&data_path).expect("open data");
            let len = f.metadata().expect("meta").len();
            f.set_len(len - 1).expect("truncate 1 byte");
        }

        let corruption = classify_physical_corruption(&data_path, 0)
            .expect("classify")
            .expect("a tear was introduced");
        assert!(
            corruption.kind.is_truncatable(),
            "a torn in-flight tail must be truncatable, got {:?}",
            corruption.kind
        );

        // Old bound (first bad record offset) walks into the incomplete output and false-reports
        // its unwritten batches as absent.
        let at_offset = validate_pack_file_bounded(&data_path, 0, None, Some(corruption.offset))
            .expect("bounded at offset");
        assert_eq!(at_offset.verdict, Verdict::Invalid, "old bound should show the false-absent");
        assert!(
            at_offset.missing_batch_count(BatchClass::Absent) > 0,
            "old bound should report absent batches (the false positive we are fixing)"
        );

        // New bound (WAL consistent end = last complete output) is clean.
        let end = super::wal_consistent_end(&data_path, 0).expect("consistent end");
        let at_end =
            validate_pack_file_bounded(&data_path, 0, None, Some(end)).expect("bounded at end");
        assert_eq!(
            at_end.verdict,
            Verdict::Valid,
            "bounding at the WAL consistent end must be Valid (no false-absent): {at_end}"
        );
    }

    /// `seal_now` seals the pack even while another handle (clone) is still alive — the graceful-
    /// shutdown case where an RPC clone outlived the sole-owner drain. The gated `close()` would
    /// have no-op'd under the clone, leaving the pack unsealed (and a WAL recovery on the next
    /// start); this asserts the forced seal ran (read-only reopen succeeds — which for a v2
    /// pack requires the clean-close sentinel) and that dropping the sibling clone afterwards
    /// does not double-seal/panic.
    #[tokio::test]
    async fn test_seal_now_seals_under_a_live_clone() {
        let temp_dir = TempDir::with_prefix("test_seal_now_clone").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Open a live (unsealed) pack and write a few outputs.
        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone())
            .expect("open append");
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..3u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            pack.save_consensus_output(output).await.expect("save output");
        }
        pack.persist().await.expect("persist");

        // Hold a second handle so the actor is NOT sole-owned, then force-seal through the clone.
        let clone = pack.clone();
        assert!(!pack.is_sole_handle(), "precondition: a clone keeps the pack non-sole");
        clone.seal_now().await;

        // The actor has sealed and exited, but `pack` (the sibling clone) is still alive. Its
        // reads/writes must fail CLEANLY on the now-closed channel — no panic, no hang, and
        // crucially no access to the truncated/unmapped file (a clone is a channel-only
        // handle, so there is no mmap to dangle). This is the direct answer to "should
        // reads/writes now error?": yes, safely.
        assert!(
            pack.get_consensus_output(1).await.is_err(),
            "a read on a clone whose actor was force-sealed must error, not touch freed memory"
        );
        assert!(!pack.contains_batch(BlockHash::default()).await);
        assert!(pack.consensus_header_by_digest(ConsensusHeaderDigest::default()).await.is_none());
        drop(clone);
        drop(pack); // Drop finds the handle already taken by seal_now -> no-op (no double-seal/panic).

        // The pack really sealed: the sentinel-gated read-only door opens and every output reads
        // back.
        let ro = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("open_static must succeed on the force-sealed pack");
        for i in 1..=3 {
            assert!(ro.get_consensus_output(i).await.is_ok(), "output {i} must read back");
        }
        ro.close().await;
    }

    /// `pack_unsealed_version` reports whether a pack carries the clean-close sentinel — the
    /// signal the `db validate` warning uses to flag a pack a writer may still be finishing,
    /// whatever its epoch number.
    #[tokio::test]
    async fn test_pack_unsealed_version_reports_seal_state() {
        let temp_dir = TempDir::with_prefix("test_unsealed_probe").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // A cleanly-sealed pack reports sealed (opened_unclean == false).
        assert_eq!(
            super::pack_unsealed_version(&data_path, 0),
            Some((PACK_VERSION, false)),
            "a sealed pack must report not-unsealed"
        );

        // After stripping the sentinel it reports unsealed.
        strip_sentinel(&data_path);
        assert_eq!(
            super::pack_unsealed_version(&data_path, 0),
            Some((PACK_VERSION, true)),
            "a sentinel-less pack must report unsealed"
        );
    }

    /// New packs are written at the current `PACK_VERSION`, the sentinel-era format: a freshly
    /// built, cleanly-closed pack reports a version at or above [`SENTINEL_MIN_VERSION`] and opens
    /// through the read-only door — which for a sentinel-era pack only succeeds when the
    /// clean-close sentinel is present, so this also proves the pack was sealed.
    #[tokio::test]
    async fn test_fresh_pack_is_sentinel_version_and_sealed() {
        let temp_dir = TempDir::with_prefix("test_fresh_sentinel").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let pack = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("a cleanly-closed current-version pack opens read-only");
        assert_eq!(pack.version, PACK_VERSION, "a fresh pack must be written at PACK_VERSION");
        assert!(
            pack.version >= super::SENTINEL_MIN_VERSION,
            "PACK_VERSION must be a sentinel-era version so new packs get crash detection"
        );
    }

    /// A v0 (batches-first) pack whose sidecar indexes were lost is RECOVERED on a writable open:
    /// the migration to v2 reorders the intact data log header-first and rebuilds the indexes from
    /// it, so the pack opens and every output reads back. (Before v0→v2 migration existed the
    /// header-first WAL replay could not rebuild a v0 log, so this was rejected with a re-sync
    /// message; migration makes it recoverable.)
    #[tokio::test]
    async fn test_open_append_migrates_v0_pack_with_lost_indexes() {
        let temp_dir = TempDir::with_prefix("test_v0_recover").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 0).await;

        // Drop the sidecar indexes; only the intact v0 data log survives.
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        for name in ["idx", "hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove index dir");
        }

        // The writable door migrates the v0 log up to v2 (reordering it and rebuilding the
        // indexes).
        let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
            .expect("open_append_exists must migrate + recover a v0 pack with lost indexes");
        pack.close().await;
        assert_eq!(peek_pack_version(&data_path), PACK_VERSION, "recovered pack must be v2");

        let ro = ConsensusPack::open_static(temp_dir.path(), 0).expect("open recovered pack");
        for i in 1..=3 {
            assert!(ro.get_consensus_output(i).await.is_ok(), "output {i} must read back");
        }
        ro.close().await;
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

        // Perf-regression guard: recovery must `mark_consistent` (and the clean `Drop` re-seal)
        // every backing file, so the next open finds them consistent instead of rebuilding the
        // whole WAL on every restart.
        assert_all_pack_files_sealed(&epoch_dir);

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

    /// Truncate an index file below its header so its next open short-reads and errors, standing in
    /// for an index that is present on disk but will not open (a corrupt/torn header). Truncation
    /// is a deterministic open failure for every index type; a zero-length file would instead
    /// be (re)created empty and never exercise the open-error path, so keep a few bytes.
    fn break_index_file(path: &std::path::Path) {
        let f = OpenOptions::new().write(true).open(path).expect("open index file to corrupt");
        f.set_len(4).expect("truncate index file header");
    }

    /// After an append open rebuilt the indexes, every output `1..=n` must be reachable again by
    /// number and by digest through a fresh read-only open — proof the pack is self-consistent.
    async fn assert_pack_reads_back(temp_dir: &TempDir, n: u64) {
        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after rebuild");
        for i in 1..=n {
            let out = pack.get_consensus_output(i).await.expect("output by number");
            assert!(
                pack.contains_consensus_header(out.consensus_header_hash()).await,
                "consensus digest for output {i} must be indexed after rebuild"
            );
            if let Some(bd) =
                out.batches().first().and_then(|c| c.batches.first()).map(|b| b.digest())
            {
                assert!(
                    pack.contains_batch(bd).await,
                    "batch digest for output {i} must be indexed after rebuild"
                );
            }
        }
    }

    /// Perf-regression guard: after a recover + clean-drop cycle, every backing file (the data
    /// log and all index files) must carry a valid clean-close sentinel — `opened_unclean()` is
    /// false. If recovery did not `mark_consistent` (and thus re-seal) a rebuilt file, that file
    /// would reopen unclean and force a full WAL rebuild on EVERY restart. Each file is read with a
    /// read-only `MmapDataFile` (whose `Drop` never seals), so the check itself never mutates
    /// state.
    fn assert_all_pack_files_sealed(epoch_dir: &std::path::Path) {
        use crate::archive::data_file::MmapDataFile;
        let files = [
            epoch_dir.join(Inner::DATA_NAME),
            epoch_dir.join(Inner::CONSENSUS_POS_NAME).join("index_pos.pdx"),
            epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.hdx"),
            epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.odx"),
            epoch_dir.join(Inner::BATCH_HASH_NAME).join("index.hdx"),
            epoch_dir.join(Inner::BATCH_HASH_NAME).join("index.odx"),
        ];
        for f in files {
            let df = MmapDataFile::open(&f, true).expect("open backing file read-only");
            assert!(
                !df.opened_unclean(),
                "backing file must be sealed after recovery (else it rebuilds every restart): {}",
                f.display(),
            );
        }
    }

    /// Build a pack, break one index file so it will not open, then reopen for append: the open
    /// must discard all indexes and rebuild them from the (intact) data log rather than
    /// aborting with an error. `rel_index` is the epoch-relative path of the index file to
    /// corrupt.
    async fn assert_corrupt_index_rebuilds_on_append(rel_index: &[&str]) {
        let temp_dir = TempDir::with_prefix("test_corrupt_index_rebuild").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 5).await;

        // Corrupt the chosen index; the data log stays untouched (it is the source of truth).
        let mut index_path = temp_dir.path().join("epoch-0");
        for part in rel_index {
            index_path = index_path.join(part);
        }
        break_index_file(&index_path);

        // Append open must not abort on the broken index — it rebuilds all indexes from the log.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open append must rebuild a broken index instead of aborting");
            pack.persist().await.expect("persist");
        }

        assert_pack_reads_back(&temp_dir, 5).await;
    }

    /// A present-but-corrupt *position* index must not abort an append open: it is rebuilt from the
    /// data log.
    #[tokio::test]
    async fn test_open_append_rebuilds_on_corrupt_position_index() {
        assert_corrupt_index_rebuilds_on_append(&["idx", "index_pos.pdx"]).await;
    }

    /// A present-but-corrupt *consensus digest* index must not abort an append open: it is rebuilt
    /// from the data log.
    #[tokio::test]
    async fn test_open_append_rebuilds_on_corrupt_consensus_digest_index() {
        assert_corrupt_index_rebuilds_on_append(&["hash", "index.hdx"]).await;
    }

    /// A present-but-corrupt *batch digest* index must not abort an append open: it is rebuilt from
    /// the data log.
    #[tokio::test]
    async fn test_open_append_rebuilds_on_corrupt_batch_digest_index() {
        assert_corrupt_index_rebuilds_on_append(&["bhash", "index.hdx"]).await;
    }

    /// The `open_append_exists` door (taken on restart for an already-created epoch) also rebuilds
    /// a broken index from the data log rather than aborting.
    #[tokio::test]
    async fn test_open_append_exists_rebuilds_on_corrupt_index() {
        let temp_dir = TempDir::with_prefix("test_corrupt_index_exists").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 4).await;

        // Break the consensus digest index; the data log is intact.
        break_index_file(&temp_dir.path().join("epoch-0").join("hash").join("index.hdx"));

        {
            let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
                .expect("open_append_exists must rebuild a broken index instead of aborting");
            pack.persist().await.expect("persist");
        }

        assert_pack_reads_back(&temp_dir, 4).await;
    }

    /// C1: a mid-save failure rolls the data log and position index back to exactly the pre-save
    /// state (no orphan records), so a retry re-saves the output cleanly with no duplicate and
    /// every output still reads back by number and digest.
    #[tokio::test]
    async fn test_save_consensus_output_rolls_back_on_failure() {
        let temp_dir = TempDir::with_prefix("test_cp_save_rollback").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Build 3 outputs directly on the Inner so we can drive the test-only failure injector.
        let mut inner =
            Inner::open_append(temp_dir.path(), &previous_epoch, committee.clone(), PACK_VERSION)
                .expect("open append");
        let mut parent = ConsensusHeader::default().digest();
        let mut outputs = Vec::new();
        for i in 0..3u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            inner.save_consensus_output(&output).expect("save");
            outputs.push(output);
        }
        assert_eq!(inner.consensus_pos_idx.len(), 3);
        let data_len_before = inner.data.file_len();

        // Output 4, saved with the injector armed: fully appended + indexed, then errors.
        let output4 = make_test_output(&committee, 3, chain.clone(), 4, parent);
        inner.fail_save_after_append = true;
        let err = inner.save_consensus_output(&output4).expect_err("injected mid-save failure");
        assert!(matches!(err, super::PackError::IndexAppend(_)), "got {err:?}");

        // Atomic rollback: the data log and the position index are back to the pre-save state.
        assert_eq!(inner.data.file_len(), data_len_before, "data log rolled back");
        assert_eq!(inner.consensus_pos_idx.len(), 3, "position index rolled back");

        // Retry the same output: it saves cleanly (the injector already cleared itself).
        inner.save_consensus_output(&output4).expect("retry saves");
        assert_eq!(inner.consensus_pos_idx.len(), 4, "output 4 now saved exactly once");

        // Exactly four Consensus records on the log — no duplicate from the rolled-back attempt.
        let consensus_records = inner
            .data
            .raw_iter()
            .expect("raw iter")
            .filter(|r| matches!(r, Ok(PackRecord::Consensus(_))))
            .count();
        assert_eq!(consensus_records, 4, "no duplicate consensus record");

        inner.persist().expect("persist");
        drop(inner); // clean close

        // Every output reads back by number and by digest through the read-only door.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
        for (i, output) in outputs.iter().chain(std::iter::once(&output4)).enumerate() {
            let number = i as u64 + 1;
            let got = pack.get_consensus_output(number).await.expect("output by number");
            assert_eq!(got.number(), number);
            assert!(
                pack.contains_consensus_header(output.consensus_header_hash()).await,
                "output {number} header must be indexed"
            );
        }
        assert!(pack.get_consensus_output(5).await.is_err(), "no phantom 5th output");
    }

    /// Build a consensus output whose single certificate's payload is exactly `batch`. Used to
    /// force the (production-impossible) case where the same batch digest appears in two different
    /// outputs, so a rolled-back save's in-place overwrite of that digest's index slot can be
    /// exercised.
    fn make_output_reusing_batch(
        committee: &Committee,
        authority_index: usize,
        number: u64,
        parent: ConsensusHeaderDigest,
        batch: Batch,
    ) -> ConsensusOutput {
        let authority =
            committee.authorities().get(authority_index).expect("authority in committee").id();
        let batch_producer = committee
            .authorities()
            .get(authority_index)
            .expect("authority in committee")
            .execution_address();
        let mut leader = Certificate::default();
        leader.update_header_author_for_test(authority);
        let builder = HeaderBuilder::from_header(leader.header()).with_payload_batch(&batch, 0_u16);
        leader.update_header_for_test(builder.build());
        leader.update_header_round_for_test(number as u32);
        leader.update_header_epoch_for_test(committee.epoch());
        let batch_digests: VecDeque<BlockHash> = std::iter::once(batch.digest()).collect();
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            number,
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
            vec![CertifiedBatch { address: batch_producer, batches: vec![batch] }],
        )
    }

    /// Regression: a rolled-back save that overwrote a duplicate batch's index slot in place
    /// must not hide that batch's earlier, still-valid copy. Save output 1 (carrying batch B);
    /// save output 2 re-using B and fail after the batch-index overwrite (the injector) →
    /// `rollback_output`; a clean close + reopen must leave B readable. The fix invalidates the
    /// digest commit marker on rollback, so the reopen rebuilds the indexes from the WAL
    /// (output 1) instead of trusting the clobbered entry. Duplicate batches across outputs
    /// cannot occur in production, so B is shared via a hand-built output. The existing
    /// rollback test retries the failed output immediately (which re-stamps the marker) and
    /// never checks the earlier batch before that retry.
    #[tokio::test]
    async fn test_rollback_keeps_previously_saved_batch_readable() {
        let temp_dir = TempDir::with_prefix("test_cp_rollback_dup_batch").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Output 1 carries batch B; capture B (digest + bytes) from it.
        let parent = ConsensusHeader::default().digest();
        let output1 = make_test_output(&committee, 0, chain.clone(), 1, parent);
        let (b_digest, b_batch) = super::collect_batches(&output1)
            .into_iter()
            .next()
            .expect("output 1 has at least one batch");
        // Output 2 re-uses B (production-impossible, so built explicitly).
        let output2 = make_output_reusing_batch(&committee, 1, 2, output1.digest(), b_batch);

        let mut inner =
            Inner::open_append(temp_dir.path(), &previous_epoch, committee.clone(), PACK_VERSION)
                .expect("open append");
        inner.save_consensus_output(&output1).expect("save output 1");
        assert!(inner.contains_batch(b_digest), "B is readable after output 1 is saved");

        // Fail output 2 after its records + index updates land (B's slot is overwritten), then roll
        // back. Do NOT retry (the production path is a fatal shutdown, not an in-process retry).
        inner.fail_save_after_append = true;
        let err = inner.save_consensus_output(&output2).expect_err("injected mid-save failure");
        assert!(matches!(err, super::PackError::IndexAppend(_)), "got {err:?}");

        inner.persist().expect("persist");
        drop(inner); // clean close seals the (now rebuild-marked) indexes

        // Reopen for append (the fatal-save restart path) → `recover_pack` rebuilds from the WAL.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("reopen for append rebuilds the indexes");
        pack.persist().await.expect("persist after rebuild");

        assert!(
            pack.contains_batch(b_digest).await,
            "batch B from output 1 must stay readable after a rolled-back duplicate save"
        );
        // Output 1 still reads back by number; the rolled-back output 2 is absent.
        assert_eq!(pack.get_consensus_output(1).await.expect("output 1 by number").number(), 1);
        assert!(pack.get_consensus_output(2).await.is_err(), "rolled-back output 2 must be absent");
    }

    /// `ConsensusPack::close` is an async drop: it must fully SEAL the pack (commit the data, sync
    /// the indexes, write the clean-close sentinels) before returning — not merely persist the
    /// data. Proof: `open_static` requires a consistent, cleanly-sealed pack (it never
    /// rebuilds), so its success right after `close().await` means the seal completed.
    #[tokio::test]
    async fn test_pack_close_seals_cleanly() {
        let temp_dir = TempDir::with_prefix("test_cp_close_seals").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..4u64 {
            let output =
                make_test_output(&committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            pack.save_consensus_output(output).await.expect("save");
        }
        // Async-close (sole reference) instead of dropping.
        pack.close().await;

        // Cleanly sealed: open_static succeeds (no rebuild) and every output reads back.
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after close");
        for i in 1..=4u64 {
            assert!(
                pack.get_consensus_output(i).await.is_ok(),
                "output {i} reads back after close"
            );
        }
    }

    /// A pack whose position index directory is gone (a crash between two index renames, or an
    /// operator's deletion) still has every output in its data log and digest indexes. A writable
    /// open must recognise the recreated, empty position index as lost and rebuild it from the
    /// log, not accept "no outputs positioned" as consistent with a log full of outputs.
    #[tokio::test]
    async fn test_open_append_rebuilds_a_lost_position_index() {
        let temp_dir = TempDir::with_prefix("test_lost_pos_index").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        std::fs::remove_dir_all(temp_dir.path().join("epoch-0").join(Inner::CONSENSUS_POS_NAME))
            .expect("remove the position index dir");
        assert!(
            ConsensusPack::open_static(temp_dir.path(), 0).is_err(),
            "the read-only door must not serve a pack whose position index is missing"
        );

        let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
            .expect("the writable open rebuilds the lost index");
        let latest = pack
            .latest_consensus_header()
            .await
            .expect("latest header")
            .expect("the rebuilt position index holds every output");
        assert_eq!(latest.number, 3, "every output must be positioned again");
        for n in 1..=3u64 {
            assert!(pack.get_consensus_output(n).await.is_ok(), "output {n} reads by number");
        }
        pack.close().await;
        ConsensusPack::open_static(temp_dir.path(), 0).expect("sealed and consistent again");
    }

    /// Every heal build stages in a directory of its own: a build whose awaiting task was
    /// cancelled may still be running when the next build of the same epoch starts, and the two
    /// must never share (or delete each other's) staging. The startup sweep removes them all.
    #[tokio::test]
    async fn test_heal_builds_stage_in_distinct_directories() {
        let temp_dir = TempDir::with_prefix("test_heal_staging").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let base_dir = temp_dir.path().join("epoch-0");
        let data_file = base_dir.join(Inner::DATA_NAME);

        let first = Inner::build_static_indexes(&base_dir, &data_file, 0).expect("first build");
        let second = Inner::build_static_indexes(&base_dir, &data_file, 0).expect("second build");
        assert_ne!(first, second, "two builds must stage in distinct directories");
        assert!(first.is_dir() && second.is_dir());

        // Installing one and abandoning the other leaves a consistent pack; the sweep removes
        // whatever staging a crash would have left behind.
        Inner::install_static_indexes(&base_dir, &first).expect("install the first build");
        ConsensusPack::open_static(temp_dir.path(), 0).expect("consistent after the install");
        assert!(second.is_dir(), "the abandoned build is untouched by the other's install");
        ConsensusPack::remove_stale_heal_dirs(&base_dir);
        assert!(!second.is_dir(), "the startup sweep removes abandoned staging");
        ConsensusPack::open_static(temp_dir.path(), 0).expect("still consistent after the sweep");
    }

    /// A data log the heal cannot open for an environmental reason (here, permissions; in
    /// production descriptor or memory exhaustion) says nothing about the pack, so the heal must
    /// surface the I/O error, which its back-off does not remember, not the at-rest-corruption
    /// verdict that would be replayed to every reader of the epoch for the back-off window.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_static_heal_open_failure_is_not_corruption() {
        use std::{io, os::unix::fs::PermissionsExt as _};

        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_heal_open_eacces").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let set_mode = |mode| {
            std::fs::set_permissions(&data_path, std::fs::Permissions::from_mode(mode))
                .expect("chmod data")
        };
        set_mode(0o000);
        if std::fs::File::open(&data_path).is_ok() {
            // Permissions are not enforced (running as root): nothing to exercise here.
            set_mode(0o644);
            return;
        }
        let result = ConsensusPack::build_static_heal(temp_dir.path(), 0);
        set_mode(0o644);
        match result {
            Err(PackError::IO(e)) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            other => panic!("expected the open's I/O error, got {other:?}"),
        }
    }

    /// A built heal that is never installed (its caller stopped waiting, or the epoch went live)
    /// removes its staging directory when dropped, rather than leaving a rebuilt index copy on disk
    /// until the next startup sweep.
    #[tokio::test]
    async fn test_uninstalled_static_heal_removes_its_staging() {
        let temp_dir = TempDir::with_prefix("test_heal_drop").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let base_dir = temp_dir.path().join("epoch-0");
        std::fs::remove_dir_all(base_dir.join(Inner::CONSENSUS_HASH_NAME)).expect("remove index");
        let staging = || {
            std::fs::read_dir(&base_dir)
                .expect("read epoch dir")
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with(Inner::REINDEX_DIR))
                .count()
        };

        let heal = ConsensusPack::build_static_heal(temp_dir.path(), 0)
            .expect("build")
            .expect("a missing index needs a heal");
        assert_eq!(staging(), 1, "the build stages its rebuilt indexes");
        drop(heal);
        assert_eq!(staging(), 0, "an uninstalled heal removes its staging");
    }

    /// When what fails the read-only open's consistency check is the position index's own last
    /// entry (its CRC), the remediation is the index one (rebuild it from the intact log), not the
    /// data log's.
    #[tokio::test]
    async fn test_open_static_damaged_last_position_entry_reports_the_index() {
        let temp_dir = TempDir::with_prefix("test_static_bad_last_pdx").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let pdx = temp_dir
            .path()
            .join("epoch-0")
            .join(Inner::CONSENSUS_POS_NAME)
            .join(Inner::CONSENSUS_POS_FILE);
        let mut bytes = std::fs::read(&pdx).expect("read pdx");
        let last_entry = bytes.len()
            - crate::archive::data_file::SENTINEL_LEN as usize
            - <super::IndexPositions as crate::archive::position_index::index::PosIndexValue>::buffer_len();
        bytes[last_entry + 3] ^= 0xFF;
        std::fs::write(&pdx, &bytes).expect("write pdx");

        let err = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect_err("a damaged position entry must fail a read-only open");
        assert!(
            err.to_string().contains("a derived index is damaged"),
            "expected the index remediation, got {err}"
        );
    }

    /// A read-only `open_static` of a sealed epoch whose position index is damaged at rest must
    /// surface the actionable `corrupt_static_index` remediation (a real error, not a clean miss) —
    /// the read-only door cannot rebuild the index, but the operator must not see the bare
    /// `LoadHeaderError`.
    #[tokio::test]
    async fn test_open_static_corrupt_index_reports_actionable_error() {
        let temp_dir = TempDir::with_prefix("test_static_corrupt_index").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 5).await;

        // Corrupt the sealed epoch's position index; the data log is intact.
        break_index_file(&temp_dir.path().join("epoch-0").join("idx").join("index_pos.pdx"));

        let err = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect_err("a damaged index must fail a read-only open");
        assert!(
            matches!(err, super::PackError::CorruptPack(_)),
            "a damaged read-only index must surface the CorruptPack remediation, got {err:?}"
        );
        assert!(
            !err.is_missing_static_files(),
            "a damaged (present) index is a real error, not a clean miss: {err:?}"
        );
    }

    /// Removing a sealed epoch's index directory entirely keeps the clean-miss classification
    /// (`is_missing_static_files`), so the import-staging-window race still resolves to "absent" —
    /// only a *damaged* (present-but-unreadable) index becomes the hard `CorruptPack` error above.
    #[tokio::test]
    async fn test_open_static_missing_index_is_a_clean_miss() {
        let temp_dir = TempDir::with_prefix("test_static_missing_index").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Remove the position index directory entirely (a genuinely absent index file).
        std::fs::remove_dir_all(temp_dir.path().join("epoch-0").join("idx"))
            .expect("remove idx dir");

        let err = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect_err("a missing index must still fail the read-only open");
        assert!(
            err.is_missing_static_files(),
            "an absent index file must classify as a clean miss, got {err:?}"
        );
    }

    // ---- db repair: repair_epoch / epoch_dirs ----

    /// A damaged index on a sealed epoch is rebuilt from the data log by
    /// `repair_epoch(apply=true)`, after which the epoch opens read-only cleanly and all
    /// outputs read back.
    #[tokio::test]
    async fn test_repair_epoch_rebuilds_broken_index() {
        let temp_dir = TempDir::with_prefix("test_repair_index").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 5).await;

        break_index_file(&temp_dir.path().join("epoch-0").join("idx").join("index_pos.pdx"));

        let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("repair must not err");
        assert!(
            matches!(outcome, EpochRepair::Repaired(_)),
            "broken index must repair, got {outcome:?}"
        );
        assert_pack_reads_back(&temp_dir, 5).await;
    }

    /// R4: `repair_epoch` must not declare a pack `Healthy` when a corrupt NON-first digest bucket
    /// slips past `open_static` (which only CRC-checks the first bucket). The full validator
    /// catches it, so repair must diagnose it, force a rebuild, and re-validate before
    /// reporting `Repaired`. Uses the same corruption as
    /// `test_validate_scans_index_bucket_crcs`.
    #[tokio::test]
    async fn test_repair_rebuilds_corrupt_nonfirst_bucket() {
        use crate::pack_validate::{validate_pack_file, Verdict};

        // On-disk width of one hdx bucket (KSIZE=32); the final BUCKET_SIZE bytes of a clean-closed
        // hdx are exactly the last (non-first) bucket.
        const HDX_BUCKET: usize = 16 + (32 + 8) * 32;

        let temp_dir = TempDir::with_prefix("test_repair_nonfirst_bucket").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let hdx_path = epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.hdx");

        // Flip a payload byte in the LAST (non-first) bucket, leaving its stamped CRC -> a corrupt
        // bucket that a first-bucket-only open cannot see.
        {
            let mut bytes = std::fs::read(&hdx_path).expect("read hdx");
            let n = bytes.len();
            bytes[n - HDX_BUCKET + 12] ^= 0xFF;
            std::fs::write(&hdx_path, &bytes).expect("write hdx");
        }

        // The bug context: open_static still succeeds, but full validation reports the corruption.
        assert!(
            ConsensusPack::open_static(temp_dir.path(), 0).is_ok(),
            "a corrupt NON-first bucket must still pass the first-bucket-only read-only open"
        );
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Invalid,
            "the full validator must detect the corrupt bucket"
        );

        // Dry run must NOT say Healthy, and must not change anything.
        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false)
            .await
            .expect("dry run must not err");
        assert!(
            matches!(dry, EpochRepair::WouldRepair(_)),
            "a corrupt non-first bucket must be WouldRepair on a dry run, got {dry:?}"
        );
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Invalid,
            "dry run must leave the corruption in place"
        );

        // Apply: repair rebuilds the index and re-validates clean.
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("apply must not err");
        assert!(
            matches!(applied, EpochRepair::Repaired(_)),
            "a corrupt non-first bucket must be Repaired on apply, got {applied:?}"
        );
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Valid,
            "after repair the pack must validate clean"
        );
        assert_pack_reads_back(&temp_dir, 3).await;
    }

    /// A writable open discards only the index that failed to open. A readable position index
    /// survives a broken digest index, and with it the attested output boundaries recovery uses
    /// to tell a torn tail from damage to acked data.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_open_append_exists_keeps_position_index_when_a_digest_index_is_broken() {
        use std::os::unix::fs::MetadataExt as _;

        let temp_dir = TempDir::with_prefix("test_selective_index_reset").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let epoch_dir = temp_dir.path().join("epoch-0");
        let pdx = epoch_dir.join(Inner::CONSENSUS_POS_NAME).join("index_pos.pdx");
        let pdx_inode = std::fs::metadata(&pdx).expect("pdx").ino();
        // A header too short to load: the consensus digest index will not open.
        std::fs::OpenOptions::new()
            .write(true)
            .open(epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.hdx"))
            .expect("open hdx")
            .set_len(16)
            .expect("truncate hdx");

        let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
            .expect("a broken digest index must not abort the writable open");
        for n in 1..=3 {
            pack.get_consensus_output(n).await.expect("output reads back after the rebuild");
        }
        pack.close().await;
        assert_eq!(
            std::fs::metadata(&pdx).expect("pdx").ino(),
            pdx_inode,
            "the readable position index must not have been discarded"
        );
    }

    /// Environmental index-open failures (resources, permissions) are surfaced instead of
    /// discarding the index; failures that describe the file itself still lead to a rebuild.
    #[test]
    fn test_is_environmental_index_error() {
        use super::PackError;
        use crate::archive::error::{load_header::LoadHeaderError, open::OpenError};
        use std::io;
        let index_io = |kind: io::ErrorKind| {
            PackError::Open(Arc::new(OpenError::IndexFileOpen(LoadHeaderError::IO(
                io::Error::from(kind),
            ))))
        };
        assert!(index_io(io::ErrorKind::PermissionDenied).is_environmental_index_error());
        assert!(index_io(io::ErrorKind::OutOfMemory).is_environmental_index_error());
        assert!(PackError::Open(Arc::new(OpenError::IndexFileOpen(LoadHeaderError::IO(
            io::Error::from_raw_os_error(libc::EMFILE)
        ))))
        .is_environmental_index_error());
        assert!(!index_io(io::ErrorKind::UnexpectedEof).is_environmental_index_error());
        assert!(!index_io(io::ErrorKind::NotFound).is_environmental_index_error());
        assert!(!PackError::Open(Arc::new(OpenError::IndexFileOpen(
            LoadHeaderError::InvalidIndexGeometry
        )))
        .is_environmental_index_error());
        assert!(!PackError::Open(Arc::new(OpenError::DataFileOpen(LoadHeaderError::IO(
            io::Error::from(io::ErrorKind::PermissionDenied)
        ))))
        .is_environmental_index_error());
    }

    /// Re-saving the output already stored under a number is an idempotent no-op (restart replay),
    /// but a DIFFERENT output under that number must be refused rather than reported as persisted
    /// while the pack keeps the other one.
    #[tokio::test]
    async fn test_save_refuses_a_conflicting_output_under_a_stored_number() {
        let temp_dir = TempDir::with_prefix("test_save_conflict").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone()).unwrap();
        let parent = ConsensusHeader::default().digest();
        let stored = make_test_output(&committee, 0, chain.clone(), 1, parent);
        let other = make_test_output(&committee, 1, chain.clone(), 1, parent);
        assert_ne!(stored.digest(), other.digest(), "fixture outputs must differ");

        let bytes = pack.save_consensus_output(stored.clone()).await.expect("save");
        assert_eq!(
            pack.save_consensus_output(stored).await.expect("idempotent re-save"),
            bytes,
            "re-saving the stored output is a no-op reporting its size"
        );
        let err = pack.save_consensus_output(other).await.expect_err("conflict must be refused");
        assert!(
            matches!(err, super::PackError::ConflictingOutput { number: 1, .. }),
            "got {err:?}"
        );
        pack.close().await;
    }

    /// An output whose batches are not exactly the set its sub-dag declares would be written but
    /// make the pack unrecoverable (replay expects exactly the declared batch records), so it is
    /// refused before anything is written.
    #[tokio::test]
    async fn test_save_refuses_an_output_whose_batches_do_not_match_its_sub_dag() {
        let temp_dir = TempDir::with_prefix("test_save_batch_set").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone()).unwrap();
        let parent = ConsensusHeader::default().digest();
        let full = make_test_output(&committee, 0, chain.clone(), 1, parent);
        let len_before = pack.data_file_len().await.expect("len");

        let with_batches = |batches: Vec<Batch>| {
            let mut certified = full.batches().to_vec();
            certified[0].batches = batches;
            ConsensusOutput::new(
                full.sub_dag().clone(),
                parent,
                1,
                false,
                full.batch_digests().clone(),
                certified,
            )
        };
        let mut partial = full.batches()[0].batches.clone();
        let dropped = partial.pop().expect("fixture has batches");
        let err = pack
            .save_consensus_output(with_batches(partial.clone()))
            .await
            .expect_err("a partial batch set must be refused");
        assert!(matches!(err, super::PackError::MissingBatches), "partial: {err:?}");

        let stray = make_test_output(&committee, 1, chain.clone(), 2, parent).batches()[0].batches
            [0]
        .clone();
        let mut extra = full.batches()[0].batches.clone();
        extra.push(stray);
        let err = pack
            .save_consensus_output(with_batches(extra))
            .await
            .expect_err("an extra batch must be refused");
        assert!(matches!(err, super::PackError::ExtraBatches), "extra: {err:?}");

        assert_eq!(pack.data_file_len().await.expect("len"), len_before, "nothing was written");
        partial.push(dropped);
        pack.save_consensus_output(with_batches(partial)).await.expect("the full set saves");
        pack.close().await;
    }

    /// An unclean pack can end in an incomplete output whose last record happens to end the file
    /// (no torn frame, no padding). Every record frames, so the physical walk finds nothing,
    /// and a full validation reports the output's missing batches. It is still the unacked
    /// in-flight write recovery truncates: `db repair` must plan that truncation (dry run and
    /// apply), not call the pack Unrepairable.
    #[tokio::test]
    async fn test_repair_truncates_incomplete_output_on_a_record_boundary() {
        use crate::pack_validate::{
            classify_physical_corruption, incomplete_trailing_output, validate_pack_file,
        };

        let temp_dir = TempDir::with_prefix("test_repair_incomplete_output").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        // Record starts: 0 meta, 1-5 output 1, 6-10 output 2, 11 output 3's header, 12-15 its
        // batches.
        let mut starts = Vec::new();
        {
            let pack =
                Pack::<PackRecord>::open(&data_path, 0, true, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            let mut iter = pack.raw_iter().expect("iter");
            loop {
                let pos = iter.logical_position();
                match iter.next() {
                    None => break,
                    Some(record) => {
                        record.expect("clean record");
                        starts.push(pos);
                    }
                }
            }
        }
        assert_eq!(starts.len(), 16, "fixture layout changed");
        // Keep output 3's header and two of its batches, ending exactly on a record boundary; the
        // cut also drops the clean-close sentinel, so the pack reads as unclean.
        OpenOptions::new()
            .write(true)
            .open(&data_path)
            .expect("open data")
            .set_len(starts[14])
            .expect("cut data");

        assert!(
            classify_physical_corruption(&data_path, 0).expect("classify").is_none(),
            "precondition: every record frames"
        );
        assert!(
            validate_pack_file(&data_path, 0, None).expect("validate").has_data_logical_issue(),
            "precondition: a full validation sees output 3's missing batches"
        );
        assert_eq!(incomplete_trailing_output(&data_path, 0), Some(starts[11]));

        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false)
            .await
            .expect("dry run must not err");
        assert!(matches!(dry, EpochRepair::WouldRepair(_)), "dry run: {dry:?}");
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("apply must not err");
        assert!(matches!(applied, EpochRepair::Repaired(_)), "apply: {applied:?}");
        assert_pack_reads_back(&temp_dir, 2).await;
        let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open repaired");
        assert!(pack.get_consensus_output(3).await.is_err(), "the incomplete output is gone");
        pack.close().await;
    }

    /// The same R4 blind spot for the POSITION index: opening a pack checks only its last entry,
    /// so a CRC-bad earlier entry passed `open_static`, validated clean, and `repair_epoch`
    /// declared the pack `Healthy`. Meanwhile by-number reads of that output failed, as did
    /// restart's walk of the index (`read_last_committed` / `count_leaders`). The validator
    /// must check every entry against the log, and repair must then rebuild the index.
    #[tokio::test]
    async fn test_repair_rebuilds_corrupt_nonlast_position_entry() {
        use crate::pack_validate::{validate_pack_file, Verdict};

        // One position entry: consensus_header, output_start, output_end (u64 each) + crc32.
        const PDX_ENTRY: usize = 28;

        let temp_dir = TempDir::with_prefix("test_repair_pdx_entry").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let pdx_path = epoch_dir.join(Inner::CONSENSUS_POS_NAME).join("index_pos.pdx");

        // Flip a byte inside the FIRST of the three entries (the sealed file ends with the three
        // entries followed by the clean-close sentinel), leaving its CRC stale.
        {
            let mut bytes = std::fs::read(&pdx_path).expect("read pdx");
            let first_entry =
                bytes.len() - crate::archive::data_file::SENTINEL_LEN as usize - 3 * PDX_ENTRY;
            bytes[first_entry + 2] ^= 0xFF;
            std::fs::write(&pdx_path, &bytes).expect("write pdx");
        }

        let pack = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect("a corrupt NON-last entry still passes the last-entry-only open");
        assert!(pack.get_consensus_output(1).await.is_err(), "output 1 must be unreadable");
        pack.close().await;
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Invalid,
            "the full validator must detect the corrupt position entry"
        );

        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false)
            .await
            .expect("dry run must not err");
        assert!(
            matches!(dry, EpochRepair::WouldRepair(_)),
            "a corrupt position entry must be WouldRepair on a dry run, got {dry:?}"
        );
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("apply must not err");
        assert!(
            matches!(applied, EpochRepair::Repaired(_)),
            "a corrupt position entry must be Repaired on apply, got {applied:?}"
        );
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Valid,
            "after repair the pack must validate clean"
        );
        assert_pack_reads_back(&temp_dir, 3).await;
    }

    /// `db repair` on a legacy v0 pack whose derived index is corrupt MIGRATES it to v2: the
    /// migration rebuilds the indexes purely from the intact data log, so the corrupt bucket is
    /// discarded and the repaired pack validates clean. The dry run predicts the apply (both
    /// actionable). (The Finding #4 "prove WAL replays before wiping the indexes" guard still
    /// governs the non-legacy v2 path in `repair_epoch`; a v0 log is now replayable via
    /// migration, so it is no longer an example of an unrebuildable pack.)
    #[tokio::test]
    async fn test_repair_migrates_legacy_v0_pack_with_corrupt_index() {
        use crate::pack_validate::{validate_pack_file, Verdict};

        const HDX_BUCKET: usize = 16 + (32 + 8) * 32;

        let temp_dir = TempDir::with_prefix("test_repair_v0_migrate").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 0).await;

        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let hdx_path = epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.hdx");

        // Corrupt a non-first bucket so `open_static` (first-bucket-only) still succeeds — the
        // corrupt-index setup. A v0 pack is not walked by the validator (only its migration reads
        // it), so the damage is judged by the repair below.
        {
            let mut bytes = std::fs::read(&hdx_path).expect("read hdx");
            let n = bytes.len();
            bytes[n - HDX_BUCKET + 12] ^= 0xFF;
            std::fs::write(&hdx_path, &bytes).expect("write hdx");
        }
        assert!(
            matches!(
                validate_pack_file(&data_path, 0, None),
                Err(super::PackError::InvalidVersion(_, 0))
            ),
            "the validator refuses to walk a v0 pack"
        );

        // Dry run predicts the apply: a legacy pack is actionable (would migrate), not
        // Unrepairable.
        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false)
            .await
            .expect("dry run must not err");
        assert!(
            matches!(dry, EpochRepair::WouldRepair(_)),
            "a legacy v0 pack must be actionable (would migrate) on a dry run, got {dry:?}"
        );

        // Apply migrates it to v2, rebuilding the indexes from the data log.
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("apply must not err");
        assert!(
            matches!(applied, EpochRepair::Repaired(_)),
            "a legacy v0 pack must be Repaired (migrated) on apply, got {applied:?}"
        );
        assert_eq!(peek_pack_version(&data_path), PACK_VERSION, "repaired pack must be v2");
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Valid,
            "the migrated pack must validate clean (corrupt bucket rebuilt from the log)"
        );
        assert!(
            ConsensusPack::open_static(temp_dir.path(), 0).is_ok(),
            "open_static must succeed on the migrated pack"
        );
    }

    /// Finding #7: a fresh pack whose first write sized the `data` file to 1 MiB of zeros but
    /// crashed before the header was durable is *unwritten*, not corrupt. A writable `open_append`
    /// must reinitialize it in place (so the node stops crash-looping at `new_epoch`) rather than
    /// failing with a bare CRC error.
    #[tokio::test]
    async fn test_open_append_reinitializes_unwritten_file() {
        let temp_dir = TempDir::with_prefix("test_unwritten_append").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Simulate the crash: an epoch dir with a 1 MiB all-zero `data` file.
        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&epoch_dir).expect("mkdir");
        std::fs::write(epoch_dir.join(Inner::DATA_NAME), vec![0_u8; 1 << 20]).expect("write zeros");

        // Writable open reinitializes the unwritten file and behaves like a fresh pack.
        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open_append must reinitialize an all-zero data file");
        let output =
            make_test_output(&committee, 0, chain.clone(), 1, ConsensusHeader::default().digest());
        pack.save_consensus_output(output).await.expect("save output");
        pack.persist().await.expect("persist");
        pack.close().await;

        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after reinit");
        assert!(pack.get_consensus_output(1).await.is_ok(), "output must read back after reinit");
    }

    /// Finding #7: a read-only door cannot reinitialize, so it must surface the all-zero file as a
    /// classifiable, actionable error rather than a bare "invalid crc32 checksum".
    #[tokio::test]
    async fn test_open_static_rejects_unwritten_file() {
        let temp_dir = TempDir::with_prefix("test_unwritten_static").expect("temp dir");
        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&epoch_dir).expect("mkdir");
        std::fs::write(epoch_dir.join(Inner::DATA_NAME), vec![0_u8; 1 << 20]).expect("write zeros");

        let err = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect_err("read-only open of an all-zero file must fail");
        assert!(err.is_unwritten_data_file(), "must be classified as unwritten, got {err:?}");
        assert!(
            err.to_string().contains("all zeros"),
            "the error must be actionable (mention 'all zeros'), got: {err}"
        );
    }

    /// Finding #7 (refinement): only a file up to the first-grow size (`initial_size` = 1 MiB) is
    /// treated as unwritten. A larger all-zero file is not a first-write artifact (growing past the
    /// first allocation requires writing a non-zero header first), so it must NOT be classified as
    /// unwritten — it falls through to the normal corrupt-header path, and the zero-scan never runs
    /// over it.
    #[tokio::test]
    async fn test_oversized_zero_file_is_not_unwritten() {
        let temp_dir = TempDir::with_prefix("test_oversized_zero").expect("temp dir");
        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&epoch_dir).expect("mkdir");
        // One byte past the 1 MiB first-grow size.
        std::fs::write(epoch_dir.join(Inner::DATA_NAME), vec![0_u8; (1 << 20) + 1])
            .expect("write zeros");

        let err = ConsensusPack::open_static(temp_dir.path(), 0)
            .expect_err("read-only open of an oversized all-zero file must fail");
        assert!(
            !err.is_unwritten_data_file(),
            "a file larger than the first grow must NOT be classified as unwritten, got {err:?}"
        );
    }

    /// Finding #7: `db repair` on an all-zero file reports `Unrepairable` (there is nothing to
    /// rebuild) with an actionable message, and does NOT mutate the file (no sealing the zeros into
    /// an 8-byte sentinel, which would erase the forensic signal).
    #[tokio::test]
    async fn test_repair_epoch_unwritten_is_unrepairable() {
        let temp_dir = TempDir::with_prefix("test_unwritten_repair").expect("temp dir");
        let epoch_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&epoch_dir).expect("mkdir");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        std::fs::write(&data_path, vec![0_u8; 1 << 20]).expect("write zeros");

        for apply in [false, true] {
            let res = ConsensusPack::repair_epoch(temp_dir.path(), 0, apply)
                .await
                .expect("repair must not err");
            match res {
                EpochRepair::Unrepairable(msg) => assert!(
                    msg.contains("all zeros"),
                    "an unwritten file must be Unrepairable with an actionable message, got: {msg}"
                ),
                other => {
                    panic!("an unwritten file must be Unrepairable (apply={apply}), got {other:?}")
                }
            }
        }
        assert_eq!(
            std::fs::metadata(&data_path).expect("stat").len(),
            1 << 20,
            "repair must not mutate the unwritten data file"
        );
    }

    /// A torn trailing tail (stray bytes appended past the sealed data) is truncated back to the
    /// last complete output by `repair_epoch`.
    #[tokio::test]
    async fn test_repair_epoch_truncates_torn_tail() {
        use std::io::Write as _;
        let temp_dir = TempDir::with_prefix("test_repair_tail").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 4).await;

        let data = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let mut f = OpenOptions::new().append(true).open(&data).expect("open data");
            f.write_all(&[1, 2, 3, 4, 5]).expect("append stray bytes");
        }
        assert!(
            ConsensusPack::open_static(temp_dir.path(), 0).is_err(),
            "a torn tail must fail the read-only open"
        );

        let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("repair must not err");
        assert!(
            matches!(outcome, EpochRepair::Repaired(_)),
            "torn tail must repair, got {outcome:?}"
        );
        assert_pack_reads_back(&temp_dir, 4).await;
    }

    /// Dry run (`apply=false`) reports what it would do and writes nothing; the pack stays damaged.
    #[tokio::test]
    async fn test_repair_epoch_dry_run_makes_no_change() {
        let temp_dir = TempDir::with_prefix("test_repair_dryrun").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 4).await;

        let pdx = temp_dir.path().join("epoch-0").join("idx").join("index_pos.pdx");
        break_index_file(&pdx);
        let before = std::fs::read(&pdx).expect("read pdx");

        let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, false)
            .await
            .expect("dry run must not err");
        assert!(
            matches!(outcome, EpochRepair::WouldRepair(_)),
            "dry run must report WouldRepair, got {outcome:?}"
        );
        assert_eq!(std::fs::read(&pdx).expect("reread pdx"), before, "dry run must not write");
        assert!(
            ConsensusPack::open_static(temp_dir.path(), 0).is_err(),
            "dry run must leave the pack damaged"
        );
    }

    /// A legacy (pre-v2) pack has no clean-close sentinel, so `db validate` judges its torn tail by
    /// what the migration to v2 would do, the same verdict `db repair`/`db migrate` reach: an
    /// unacked tail past the last complete output is truncatable, damage below the pack's length
    /// attestation is refused.
    #[tokio::test]
    async fn test_legacy_tail_verdict_matches_the_migration() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        use crate::pack_validate::{
            classify_physical_corruption, recovery_refusal, CorruptionKind,
        };
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // An unacked torn record after the last complete output.
        let temp_dir = TempDir::with_prefix("test_legacy_torn_tail").expect("temp dir");
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let mut f = OpenOptions::new().append(true).open(&data_path).expect("open data");
            f.write_all(&[64, 0, 0, 0, 1, 2, 3]).expect("append torn record");
        }
        let corruption = classify_physical_corruption(&data_path, 0)
            .expect("classify")
            .expect("a torn tail is detected");
        assert_eq!(corruption.kind, CorruptionKind::TornTrailingTail);
        assert_eq!(recovery_refusal(&data_path, 0), None, "the migration drops this tail");
        let dry = ConsensusPack::migrate_epoch(temp_dir.path(), 0, false).await.expect("dry run");
        assert!(matches!(dry, EpochMigrate::WouldMigrate(_)), "migration dry run: {dry:?}");

        // A committed output damaged in place: the pack's indexes still attest its full length.
        let temp_dir = TempDir::with_prefix("test_legacy_mid_log").expect("temp dir");
        build_test_pack_version(&temp_dir, &committee, &chain, &previous_epoch, 3, 1).await;
        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary + 8)).expect("seek into output 2");
            f.write_all(&[0xFF; 4]).expect("damage output 2");
        }
        assert!(recovery_refusal(&data_path, 0).is_some(), "damage below the attested length");
    }

    /// A sealed pack's derived indexes form one set: with any one of them missing the read-only
    /// open refuses the pack, so validation must not call it `Valid`. A bare data file (no
    /// derived indexes at all) still validates on its own.
    #[tokio::test]
    async fn test_validate_reports_a_partial_index_set() {
        use crate::pack_validate::{validate_pack_file, PackIssue, Verdict};
        let temp_dir = TempDir::with_prefix("test_validate_partial_index").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        assert_eq!(
            validate_pack_file(&data_path, 0, None).expect("validate").verdict,
            Verdict::Valid
        );

        for name in Inner::INDEX_DIRS {
            let aside = epoch_dir.join(format!("{name}.aside"));
            std::fs::rename(epoch_dir.join(name), &aside).expect("move index aside");
            let report = validate_pack_file(&data_path, 0, None).expect("validate");
            assert_eq!(report.verdict, Verdict::Invalid, "{name} missing: {report}");
            assert!(
                report.issues.iter().any(|i| matches!(i, PackIssue::IndexUnreadable { .. })),
                "{name} missing: {report}"
            );
            std::fs::rename(&aside, epoch_dir.join(name)).expect("restore index");
        }

        let bare_dir = TempDir::with_prefix("test_validate_bare_data").expect("temp dir");
        let bare = bare_dir.path().join(Inner::DATA_NAME);
        std::fs::copy(&data_path, &bare).expect("copy data file");
        assert_eq!(validate_pack_file(&bare, 0, None).expect("validate").verdict, Verdict::Valid);
    }

    /// A clean, sealed epoch is `Healthy` and is left byte-for-byte untouched.
    #[tokio::test]
    async fn test_repair_epoch_healthy_is_untouched() {
        let temp_dir = TempDir::with_prefix("test_repair_healthy").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let data = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let before = std::fs::read(&data).expect("read data");

        let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("repair must not err");
        assert!(matches!(outcome, EpochRepair::Healthy), "clean epoch is Healthy, got {outcome:?}");
        assert_eq!(std::fs::read(&data).expect("reread data"), before, "healthy epoch untouched");
    }

    /// A torn epoch-meta with no outputs behind it is `Unrepairable` (the committee can't be
    /// rebuilt locally) and is left untouched.
    #[tokio::test]
    async fn test_repair_epoch_torn_meta_is_unrepairable() {
        let temp_dir = TempDir::with_prefix("test_repair_torn_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 2).await;

        // Truncate into the meta record: a dataless torn meta.
        let data = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let f = OpenOptions::new().write(true).open(&data).expect("open data");
            f.set_len(DATA_HEADER_BYTES as u64 + 2).expect("truncate into the meta");
        }
        let before = std::fs::read(&data).expect("read data");

        let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, true)
            .await
            .expect("repair must not err");
        assert!(
            matches!(outcome, EpochRepair::Unrepairable(_)),
            "a torn meta is unrepairable, got {outcome:?}"
        );
        assert_eq!(
            std::fs::read(&data).expect("reread data"),
            before,
            "unrepairable epoch untouched"
        );
    }

    /// Mid-log corruption — a damaged byte with valid records still behind it — is `Unrepairable`
    /// at the `repair_epoch` level in BOTH dry-run and apply modes (truncation would drop
    /// durably-committed outputs), and the pack is left byte-for-byte untouched. This is the
    /// `repair_epoch`-level companion to `test_recover_mid_log_corruption_errors`, which asserts
    /// the same damage errors at the lower `open_append`/`recover_pack` level.
    #[tokio::test]
    async fn test_repair_epoch_mid_log_is_unrepairable() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        let temp_dir = TempDir::with_prefix("test_repair_mid_log").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Flip a byte inside output 2's header, a few bytes past the 4-byte record size prefix so
        // the framing stays intact and output 2's batches + output 3 still decode AFTER the
        // damage — the signature of mid-log corruption (valid records past the tear) rather
        // than a torn tail.
        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let data = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&data).expect("open data");
            f.seek(SeekFrom::Start(boundary + 20)).expect("seek");
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).expect("read");
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(boundary + 20)).expect("seek back");
            f.write_all(&byte).expect("write");
        }
        // Force `repair_epoch` past its read-only `open_static` "healthy" short-circuit into the
        // physical-corruption classifier: drop the digest indexes so the open fails. (A sealed
        // pack's interior bit-flip is otherwise silent to `open_static`, which trusts the seal +
        // indexes; it surfaces only at read time / via `db validate`.) This models damaged indexes
        // sitting atop mid-log-corrupt data — repair must refuse, not rebuild indexes over the
        // corruption.
        for name in ["hash", "bhash"] {
            std::fs::remove_dir_all(temp_dir.path().join("epoch-0").join(name))
                .expect("remove digest dir");
        }
        let before = std::fs::read(&data).expect("read data");

        // Dry run and apply both classify Unrepairable (the mid-log arm returns before the
        // apply/dry-run split) and neither may touch the file.
        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false).await.expect("dry run");
        assert!(
            matches!(dry, EpochRepair::Unrepairable(_)),
            "mid-log dry run must be Unrepairable, got {dry:?}"
        );
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true).await.expect("apply");
        assert!(
            matches!(applied, EpochRepair::Unrepairable(_)),
            "mid-log apply must be Unrepairable, got {applied:?}"
        );
        assert_eq!(
            std::fs::read(&data).expect("reread data"),
            before,
            "an unrepairable mid-log pack must be left untouched"
        );
    }

    /// `epoch_dirs` lists only `epoch-{N}` directories (not files, not `staging-*`), sorted
    /// ascending.
    #[test]
    fn test_epoch_dirs_enumerates_sorted() {
        let temp_dir = TempDir::with_prefix("test_epoch_dirs").expect("temp dir");
        for n in [2u32, 0, 10, 1] {
            std::fs::create_dir_all(temp_dir.path().join(format!("epoch-{n}"))).expect("mkdir");
        }
        std::fs::create_dir_all(temp_dir.path().join("staging-3")).expect("mkdir"); // ignored
        std::fs::write(temp_dir.path().join("epoch-99"), b"a file, not a dir").expect("write"); // ignored

        // A symlinked `epoch-N/` must be enumerated (the node opens epochs by path and follows
        // symlinks); `path().is_dir()` follows the link where `file_type().is_dir()` did not.
        #[cfg(unix)]
        {
            let target = temp_dir.path().join("real-epoch-5");
            std::fs::create_dir_all(&target).expect("mkdir target");
            std::os::unix::fs::symlink(&target, temp_dir.path().join("epoch-5"))
                .expect("symlink epoch-5");
        }

        let epochs = ConsensusPack::epoch_dirs(temp_dir.path()).expect("list epochs");
        #[cfg(unix)]
        assert_eq!(epochs, vec![0, 1, 2, 5, 10]);
        #[cfg(not(unix))]
        assert_eq!(epochs, vec![0, 1, 2, 10]);
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

    /// Finding #37: a second, non-leading `EpochMeta` in the data log is append-order-impossible
    /// (each pack is written with exactly one meta, first). `replay_wal` must reject it as
    /// `CorruptPack` rather than silently folding it into the consistent prefix — which would leave
    /// the on-disk log validating as damaged after any repair (validate flags the stray meta, so
    /// `repair_epoch` would rebuild and then return `Unrepairable`). Mirrors
    /// `test_recover_mid_log_corruption_errors`.
    #[tokio::test]
    async fn test_recover_rejects_stray_mid_log_epoch_meta() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{EpochMeta, PackError, PackRecord},
        };
        let temp_dir = TempDir::with_prefix("test_recover_stray_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Frame a stray EpochMeta record exactly as the writer would (a hand-rolled size prefix
        // would desync the reader): write one into a scratch pack, then lift its bytes past
        // the data-file header. Read after the pack drops so the file is truncated to
        // `[header][record][sentinel]`; the trailing clean-close sentinel is harmless —
        // replay rejects the stray meta well before it reaches the tail.
        let scratch = temp_dir.path().join("scratch_meta");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&scratch, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open scratch");
            pack.append(&PackRecord::EpochMeta(EpochMeta {
                epoch: 0,
                committee: committee.clone(),
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            }))
            .expect("append stray meta");
            pack.commit().expect("commit scratch");
        }
        let stray_meta_bytes =
            std::fs::read(&scratch).expect("read scratch")[DATA_HEADER_BYTES..].to_vec();

        // Overwrite from the end of the last output (output 3) with the framed stray meta,
        // clobbering the clean-close sentinel that sits there. Appending past the sentinel
        // instead would leave its 8 bytes between output 3 and the stray meta, and replay
        // would stop at them as a torn tail before ever reaching the meta.
        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(3).await.expect("output 3 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary)).expect("seek to output 3 end");
            f.write_all(&stray_meta_bytes).expect("write stray meta bytes");
        }
        // Force recovery (replay_wal) to run by dropping the digest indexes.
        for name in ["hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove digest dir");
        }

        let res =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(res, Err(PackError::CorruptPack(_))),
            "a stray mid-log EpochMeta must error, got {res:?}"
        );
    }

    /// Finding #19: a corrupted mid-log record *size prefix* (not payload) desyncs the size-walking
    /// probe, so `output_after_tear` cannot reach the intact outputs after the damage. On an
    /// unclean pack with no commit marker the position index is the only desync-immune witness
    /// — a still-attested output past the replay's stopping point makes recovery reject with
    /// `CorruptPack` rather than silently truncating committed outputs. (Contrast
    /// `test_recover_mid_log_corruption_errors`, which corrupts the payload with framing intact and
    /// is caught by the walk itself.)
    #[tokio::test]
    async fn test_recover_size_prefix_corruption_errors_via_index() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_recover_size_prefix").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // Output 2 begins where output 1 ends; overwrite its 4-byte record size prefix (not the
        // payload) with a small bogus size so the size-walking probe jumps to a misaligned offset
        // and cannot re-sync to output 3.
        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary)).expect("seek to size prefix");
            f.write_all(&7u32.to_le_bytes()).expect("corrupt size prefix");
        }
        // Strip the clean-close sentinel so the pack opens unclean (forcing recovery) with NO
        // commit marker — a sealed pack's tail holds none — leaving the (intact) position
        // index as the only witness. Keep the digest indexes so recovery receives an intact
        // position index.
        {
            let f = OpenOptions::new().write(true).open(&data_path).expect("open data to unseal");
            let len = f.metadata().expect("metadata").len();
            f.set_len(len - crate::archive::data_file::SENTINEL_LEN).expect("strip sentinel");
        }

        let res =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(res, Err(PackError::CorruptPack(_))),
            "size-prefix corruption with a surviving attested output must error, got {res:?}"
        );
    }

    /// Finding #19 (read-only path): the classifier must not misread size-prefix corruption as a
    /// truncatable tail. With a surviving attested output past the damage,
    /// `classify_physical_corruption` reports `MidLogCorruption` (DATA LOSS / re-sync), not
    /// `TornTrailingTail` ("SAFE") — via the position index, immune to the size-walk desync.
    #[tokio::test]
    async fn test_classify_size_prefix_corruption_is_mid_log() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        let temp_dir = TempDir::with_prefix("test_classify_size_prefix").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary)).expect("seek to size prefix");
            f.write_all(&7u32.to_le_bytes()).expect("corrupt size prefix");
        }
        // Unclean (stripped sentinel) so the pre-fix walk would call a torn tail "SAFE"; the
        // position index (kept intact) attests output 3 survives past the damage.
        {
            let f = OpenOptions::new().write(true).open(&data_path).expect("open data to unseal");
            let len = f.metadata().expect("metadata").len();
            f.set_len(len - crate::archive::data_file::SENTINEL_LEN).expect("strip sentinel");
        }

        let corruption = crate::pack_validate::classify_physical_corruption(&data_path, 0)
            .expect("classify")
            .expect("corruption detected");
        assert_eq!(
            corruption.kind,
            crate::pack_validate::CorruptionKind::MidLogCorruption,
            "size-prefix corruption with a surviving attested output must classify as mid-log, got \
             {:?}",
            corruption.kind
        );
    }

    /// The read-only checks behind `db validate` and `db repair`'s dry run must attest the same
    /// output boundaries as the writable open, including from a position index that was not
    /// cleanly closed. Such an index is still capacity-padded, so its tail is not a whole number
    /// of entries and a read-only index open refuses it; the checks read its entries straight
    /// from the file instead. Here a corrupted size prefix hides an intact later output from the
    /// WAL walk and no commit marker survives, so only the position index shows that the damage
    /// lies below acked data: every verdict must be the writable open's refusal.
    #[tokio::test]
    async fn test_dry_run_matches_apply_on_unsealed_padded_position_index() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        use crate::{
            consensus_pack::{check_recoverable, PackError},
            pack_validate::{classify_physical_corruption, recovery_refusal, CorruptionKind},
        };
        let temp_dir = TempDir::with_prefix("test_unsealed_pdx_parity").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(boundary)).expect("seek to size prefix");
            f.write_all(&7u32.to_le_bytes()).expect("corrupt size prefix");
        }
        strip_sentinel(&data_path);
        // The position index as a crash leaves it: no sentinel, zero-padded to its capacity.
        let pdx_path = epoch_dir.join(Inner::CONSENSUS_POS_NAME).join("index_pos.pdx");
        strip_sentinel(&pdx_path);
        OpenOptions::new()
            .write(true)
            .open(&pdx_path)
            .expect("open pdx")
            .set_len(1 << 20)
            .expect("pad pdx");

        assert!(
            check_recoverable(&data_path, 0).is_err(),
            "the read-only recovery check must refuse what the writable open refuses"
        );
        assert!(recovery_refusal(&data_path, 0).is_some(), "validate must not call it truncatable");
        let corruption = classify_physical_corruption(&data_path, 0)
            .expect("classify")
            .expect("corruption detected");
        assert_eq!(corruption.kind, CorruptionKind::MidLogCorruption);
        let dry = ConsensusPack::repair_epoch(temp_dir.path(), 0, false).await.expect("dry run");
        assert!(matches!(dry, EpochRepair::Unrepairable(_)), "dry run: {dry:?}");
        let applied = ConsensusPack::repair_epoch(temp_dir.path(), 0, true).await.expect("apply");
        assert!(matches!(applied, EpochRepair::Unrepairable(_)), "apply: {applied:?}");
        let res =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(matches!(res, Err(PackError::CorruptPack(_))), "writable open: {res:?}");
    }

    /// R1 regression: a *failed* recovery must be idempotent and non-destructive. Recovery
    /// validates the data-log WAL (index-free) BEFORE it touches any index or truncates the
    /// log, so a detected corruption returns without mutating on-disk state and a retry (a
    /// plain node restart, or `db repair`) re-derives the SAME reject from the unchanged log.
    /// Here output 2 is torn with output 3 decodable after it (corruption of committed data);
    /// both attempts must reject with `CorruptPack`, and the data bytes must survive untouched
    /// across both.
    #[tokio::test]
    async fn test_failed_recovery_preserves_data_on_retry() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_failed_recovery_retry").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let boundary = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        // The committed data length (all three outputs + the clean-close sentinel) that every
        // recovery attempt must preserve. Flipping one byte below does not change it.
        let committed_len = std::fs::metadata(&data_path).expect("metadata").len();

        // Corrupt a byte inside output 2's header payload (past the 4-byte size prefix, so the
        // framing stays intact and output 3 still decodes AFTER the damage -> provably not the
        // final record).
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
        // Force recovery by dropping the digest indexes (so `files_consistent` fails). Detection is
        // index-free: output 3 decoding past the torn output 2 is corruption regardless of what any
        // index says.
        for name in ["hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove digest dir");
        }

        // First attempt: correctly rejects, and leaves the data bytes intact.
        let first =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(first, Err(PackError::CorruptPack(_))),
            "first recovery must reject mid-log corruption, got {first:?}"
        );
        assert_eq!(
            std::fs::metadata(&data_path).expect("metadata").len(),
            committed_len,
            "a rejected recovery must not truncate the data log"
        );

        // Second attempt (a plain restart): must STILL reject and STILL preserve the data. This is
        // the regression: pre-fix, the first attempt had reduced the durable watermark, so this
        // open truncated outputs 2 and 3 back to output 1 and returned Ok.
        let second =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(second, Err(PackError::CorruptPack(_))),
            "the retry must also reject, not silently truncate committed data, got {second:?}"
        );
        assert_eq!(
            std::fs::metadata(&data_path).expect("metadata").len(),
            committed_len,
            "the retry must preserve committed outputs 2 and 3 (no truncation back to output 1)"
        );
    }

    /// Index-free corruption detection (the #5 regression). `persist()` acks the DATA (msync)
    /// without syncing indexes, so after a crash the indexes are stale; the old
    /// index-attested-end watermark then collapsed and *silently truncated* committed
    /// outputs. Here output 2 is damaged at rest but output 3 — a complete LATER output — still
    /// decodes past the tear. With EVERY index deleted (the post-crash state and proof no index is
    /// consulted), `output_after_tear` sees output 3's header: an output written past the tear
    /// implies output 2 was durably committed first, so this is corruption of committed data and
    /// recovery must reject it, not drop outputs 2-3.
    #[tokio::test]
    async fn test_recover_corruption_before_a_later_output_is_index_free() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_recover_index_free_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        // End of output 1 is where a naive replay stops once output 2 is torn.
        let output1_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(1).await.expect("output 1 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let full_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(full_len > output1_end, "outputs 2 and 3 must extend past output 1");

        // Corrupt a byte inside output 2's header payload (past the 4-byte size prefix, so the
        // framing stays intact and output 3 — a complete later output — still decodes AFTER it).
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
        // Delete EVERY index (the post-crash state, and proof the detection needs no index at all).
        for name in ["hash", "bhash", "idx"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove index dir");
        }

        // A later complete output decodes past the tear → committed data was damaged → reject.
        let result =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(result, Err(PackError::CorruptPack(_))),
            "corruption before a later committed output must be rejected, got {result:?}"
        );
        // A rejected recovery mutates nothing, so the data log is untouched.
        assert_eq!(
            std::fs::metadata(&data_path).expect("metadata").len(),
            full_len,
            "a rejected recovery must not truncate the data log"
        );
    }

    /// Build `n` outputs and `persist()` (which stamps the tail commit marker) via the `Inner`,
    /// then leak it so no clean close runs — an unclean data file with the marker intact on
    /// disk. Returns the end offset of output `n-1` (the last complete boundary once output
    /// `n`'s header is torn).
    fn build_unclean_pack_with_marker(
        temp_dir: &TempDir,
        committee: &Committee,
        chain: &Arc<RethChainSpec>,
        previous_epoch: &EpochRecord,
        n: u64,
    ) -> u64 {
        let mut inner =
            Inner::open_append(temp_dir.path(), previous_epoch, committee.clone(), PACK_VERSION)
                .expect("open_append inner");
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..n {
            let output =
                make_test_output(committee, (i % 4) as usize, chain.clone(), i + 1, parent);
            parent = output.digest();
            inner.save_consensus_output(&output).expect("save output");
        }
        inner.persist().expect("persist stamps the marker");
        let prev_end = inner.output_end_for_consensus(n - 1).expect("boundary");
        std::mem::forget(inner); // unclean exit: skip the clean close so the marker survives
        prev_end
    }

    /// Flip one byte at `pos` in the file at `path` (corruption at rest).
    fn corrupt_byte_at(path: &std::path::Path, pos: u64) {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
        let mut f = OpenOptions::new().read(true).write(true).open(path).expect("open data");
        f.seek(SeekFrom::Start(pos)).expect("seek");
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).expect("read");
        byte[0] ^= 0xFF;
        f.seek(SeekFrom::Start(pos)).expect("seek back");
        f.write_all(&byte).expect("write");
    }

    /// The tail commit marker closes the residual the structural probe cannot see: at-rest
    /// corruption of the LAST committed output with nothing decodable after it. The marker records
    /// the durable acked end, so a WAL replay that stops below it means committed data was damaged.
    #[tokio::test]
    async fn test_recover_last_output_corruption_caught_by_commit_marker() {
        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_marker_last_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Unclean pack; the marker records committed_end == output 3's end.
        let output2_end =
            build_unclean_pack_with_marker(&temp_dir, &committee, &chain, &previous_epoch, 3);
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // Corrupt output 3's header (past its 4-byte size prefix): replay stops at output 2 and no
        // complete output decodes after, so only the marker can flag it.
        corrupt_byte_at(&data_path, output2_end + 20);

        // `db repair`'s dry run must predict that refusal rather than report `WouldRepair`, and
        // the apply must refuse without changing the pack.
        let bytes_before = std::fs::read(&data_path).expect("read data");
        for apply in [false, true] {
            let verdict = ConsensusPack::repair_epoch(temp_dir.path(), 0, apply)
                .await
                .expect("repair must not err");
            assert!(
                matches!(verdict, EpochRepair::Unrepairable(_)),
                "repair (apply={apply}) must report the damaged acked output, got {verdict:?}"
            );
        }
        assert_eq!(std::fs::read(&data_path).expect("read data"), bytes_before, "pack changed");

        let result = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee);
        assert!(
            matches!(result, Err(PackError::CorruptPack(_))),
            "the commit marker must catch at-rest corruption of the last output, got {result:?}"
        );
    }

    /// Fail-safe degrade: with the marker cleared (a power loss that lost the best-effort write),
    /// the same last-output corruption is indistinguishable from an unacked torn tail, so
    /// recovery truncates it rather than raising a false error (the pre-marker / probe-only
    /// behavior).
    #[tokio::test]
    async fn test_recover_last_output_corruption_without_marker_truncates() {
        use std::io::{Seek as _, SeekFrom, Write as _};
        let temp_dir = TempDir::with_prefix("test_marker_last_nomark").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        let output2_end =
            build_unclean_pack_with_marker(&temp_dir, &committee, &chain, &previous_epoch, 3);
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        corrupt_byte_at(&data_path, output2_end + 20);
        // Clear the marker (zero the last 16 bytes of the padded, unclean file).
        {
            let mut f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open");
            f.seek(SeekFrom::End(-16)).expect("seek end");
            f.write_all(&[0u8; 16]).expect("clear marker");
        }

        let pack = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee)
            .expect("without the marker, recovery truncates the torn last output");
        assert!(pack.get_consensus_output(2).await.is_ok(), "output 2 survives");
        assert!(pack.get_consensus_output(3).await.is_err(), "torn output 3 truncated");
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

    /// A torn epoch-meta with no outputs behind it holds no committed data, but it is not a
    /// truncatable tail: both open doors refuse it, so `db validate` must not report it as one.
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
        assert!(!c.kind.is_truncatable(), "a torn epoch-meta is refused, not truncated");
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

    /// A CRC failure in the FINAL record of a cleanly-SEALED pack is at-rest corruption (bit rot),
    /// not a truncatable tail — the clean-close sentinel proves the log was complete. `db validate`
    /// must classify it `CorruptSealedRecord` (data loss), NOT `TornTrailingTail` ("SAFE").
    #[tokio::test]
    async fn test_classify_physical_corruption_corrupt_sealed_record() {
        use crate::pack_validate::{classify_physical_corruption, CorruptionKind};
        let temp_dir = TempDir::with_prefix("test_classify_sealed_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let output3_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(3).await.expect("output 3 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // Flip a payload byte of the LAST record (past its size prefix, before its CRC): the record
        // framing and the clean-close sentinel stay intact, so the pack is still SEALED and nothing
        // decodes after the damage.
        corrupt_byte_at(&data_path, output3_end - 8);
        let c = classify_physical_corruption(&data_path, 0).expect("classify").expect("corruption");
        assert_eq!(c.kind, CorruptionKind::CorruptSealedRecord);
        assert!(!c.kind.is_truncatable(), "a corrupt record in a sealed pack is not truncatable");
        assert!(!c.decodable_after, "nothing decodes after the last record");
    }

    /// `db repair --force` (and its dry run) must NOT truncate a sealed pack's bit-rotted committed
    /// output — it reports `Unrepairable` and leaves the data untouched.
    #[tokio::test]
    async fn test_repair_epoch_sealed_corrupt_record_is_unrepairable() {
        let temp_dir = TempDir::with_prefix("test_repair_sealed_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let output3_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(3).await.expect("output 3 end")
        };
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let full_len = std::fs::metadata(&data_path).expect("metadata").len();
        corrupt_byte_at(&data_path, output3_end - 8);

        for apply in [false, true] {
            let outcome = ConsensusPack::repair_epoch(temp_dir.path(), 0, apply)
                .await
                .expect("repair must not err");
            assert!(
                matches!(outcome, EpochRepair::Unrepairable(_)),
                "sealed bit-rot must be Unrepairable (apply={apply}), got {outcome:?}"
            );
            assert_eq!(
                std::fs::metadata(&data_path).expect("metadata").len(),
                full_len,
                "an unrepairable pack must not be truncated (apply={apply})"
            );
        }
    }

    /// The recovery authority itself refuses a sealed pack's corrupt record (guard, not just the
    /// CLI messaging): with the digest indexes deleted `files_consistent` fails and
    /// `recover_pack` replays the WAL, where the sealed-log guard errors instead of truncating.
    #[tokio::test]
    async fn test_recover_pack_refuses_sealed_corrupt_record() {
        use crate::consensus_pack::PackError;
        let temp_dir = TempDir::with_prefix("test_recover_sealed_corrupt").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;
        let output3_end = {
            let pack = ConsensusPack::open_static(temp_dir.path(), 0).expect("open static");
            pack.consensus_output_end(3).await.expect("output 3 end")
        };
        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        corrupt_byte_at(&data_path, output3_end - 8);
        // Delete the digest indexes so `files_consistent` fails and `recover_pack` replays the WAL.
        for name in ["hash", "bhash"] {
            std::fs::remove_dir_all(epoch_dir.join(name)).expect("remove digest dir");
        }
        let result = ConsensusPack::open_append_exists(temp_dir.path(), 0);
        assert!(
            matches!(result, Err(PackError::CorruptPack(_))),
            "recover_pack must refuse a sealed pack's corrupt record, got {result:?}"
        );
    }

    /// A pack whose first record is torn (a crash mid meta append left only part of the size
    /// prefix), even with nothing indexed behind it, is an invalid pack: `open_append` rejects it
    /// rather than truncating and rewriting the meta.  The meta is committed the instant it is
    /// written, so a torn meta is not a normal state; the operator removes the epoch dir and it
    /// rebuilds.
    #[tokio::test]
    async fn test_open_append_rejects_dataless_torn_first_record() {
        let temp_dir = TempDir::with_prefix("test_cp_torn_meta").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Open + persist a clean meta-only pack, then DROP it so the on-disk file is exactly its
        // logical length before we tear it.
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

        // Tear the meta record mid size-prefix: record bytes exist past the header, but the
        // first record cannot be read and no output was ever committed behind it.
        let torn_len = DATA_HEADER_BYTES as u64 + 2;
        {
            let f = OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.set_len(torn_len).expect("truncate");
        }

        let result =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(result, Err(super::PackError::EpochLoad(_))),
            "a torn meta must be rejected, got {result:?}"
        );

        // No repair happened: the meta was not rewritten, so reopening still fails the same way
        // (the old heal would have made this second open succeed).
        let again = ConsensusPack::open_append(temp_dir.path(), previous_epoch, committee.clone());
        assert!(
            matches!(again, Err(super::PackError::EpochLoad(_))),
            "a torn meta must stay invalid across reopens (no repair), got {again:?}"
        );
    }

    /// The export copies the pack's `data` file bounded to its logical length (`data_file_len`),
    /// not to physical EOF, so it never captures the mmap capacity padding — no
    /// reconcile/truncate — and a concurrent append after the length is captured cannot corrupt
    /// the copy. This is the padding-immunity + re-growth-immunity the reconcile-then-copy
    /// sequence achieved only under a fragile "epoch is quiescent" assumption.
    #[tokio::test]
    async fn test_bounded_copy_of_padded_pack_stream_imports() {
        use std::io::Read as _;

        let temp_dir = TempDir::with_prefix("test_cp_bounded_copy").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);

        let pack =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone())
                .expect("open pack");
        let num_outputs = 3usize;
        let mut outputs = Vec::new();
        let mut parent = ConsensusHeader::default().digest();
        for i in 0..num_outputs {
            let output = make_test_output(&committee, i % 4, chain.clone(), i as u64 + 1, parent);
            parent = output.digest();
            outputs.push(output.clone());
            pack.save_consensus_output(output).await.unwrap();
        }
        pack.persist().await.expect("persist");

        // The live pack keeps its mmap capacity padding: the physical file is larger than the
        // logical data length the export bounds its copy to.
        let data_len = pack.data_file_len().await.expect("data len");
        let padded_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            padded_len > data_len,
            "a live pack must be physically padded ({padded_len} > logical {data_len})"
        );

        // Append MORE after capturing `data_len`. The bounded copy must still yield exactly the
        // first three outputs: `[0, data_len)` is immutable append-only data, so a later append
        // (which only extends past `end`) cannot leak into the copy.
        let extra = make_test_output(
            &committee,
            0,
            chain.clone(),
            num_outputs as u64 + 1,
            outputs[num_outputs - 1].digest(),
        );
        pack.save_consensus_output(extra).await.unwrap();
        pack.persist().await.expect("persist extra");

        // Bounded copy of exactly `data_len` bytes — what the export does (no reconcile/truncate).
        let bundle_dir = TempDir::with_prefix("test_cp_bounded_bundle").expect("temp dir");
        let copy_dst = bundle_dir.path().join("consensus_data");
        {
            let mut src = std::fs::File::open(&data_path).expect("open src");
            let mut dst = std::fs::File::create(&copy_dst).expect("create dst");
            std::io::copy(&mut (&mut src).take(data_len), &mut dst).expect("bounded copy");
        }
        assert_eq!(
            std::fs::metadata(&copy_dst).expect("metadata").len(),
            data_len,
            "the copy must be exactly the logical length, not the padded physical size"
        );

        let dst_dir = TempDir::with_prefix("test_cp_bounded_dst").expect("temp dir");
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
        .expect("bounded copy of a padded pack must stream_import");
        wait_for(
            async || imported.get_consensus_output(num_outputs as u64).await.is_ok(),
            "last stream-imported output to be readable",
        )
        .await;
        for (i, expected) in outputs.iter().enumerate().take(num_outputs) {
            let got = imported.get_consensus_output(i as u64 + 1).await.unwrap();
            compare_outputs(&got, expected);
        }
        // The output appended after the snapshot must NOT be in the bounded copy.
        assert!(
            imported.get_consensus_output(num_outputs as u64 + 1).await.is_err(),
            "a post-snapshot append must not appear in the bounded copy"
        );
        drop(imported);
        drop(pack);
    }

    /// `db validate` (via `validate_pack_file`) must scan the sidecar digest indexes' bucket CRCs —
    /// the only detector for a lost/corrupt or zeroed hdx bucket, which nothing else runs. A clean
    /// pack scans clean; a payload flip reports `corrupt` and a whole-bucket zero reports `dirty`
    /// (the launder-prone lost-page shape), both flipping the verdict to Invalid; a bare data file
    /// with no sidecar dirs is validated data-only (`index_scan = None`).
    #[tokio::test]
    async fn test_validate_scans_index_bucket_crcs() {
        use crate::pack_validate::{validate_pack_file, Verdict};

        // The on-disk width of one hdx bucket (KSIZE=32): 16 + (32+8)*32. A clean-closed hdx is
        // `header + bloom + buckets*BUCKET_SIZE` with no trailing bytes, so the final BUCKET_SIZE
        // bytes are exactly the last bucket — corrupt there without depending on the header/bloom
        // sizes (the bloom size is feature-gated).
        const HDX_BUCKET: usize = 16 + (32 + 8) * 32;

        let temp_dir = TempDir::with_prefix("test_validate_index_scan").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let epoch_dir = temp_dir.path().join("epoch-0");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let hdx_path = epoch_dir.join(Inner::CONSENSUS_HASH_NAME).join("index.hdx");

        // Clean pack: the sidecar indexes are scanned and clean; verdict Valid.
        let report = validate_pack_file(&data_path, 0, None).expect("validate clean pack");
        let scan = report.index_scan.expect("sidecar indexes must be scanned when present");
        assert!(scan.is_clean(), "a clean-closed index has no dirty/corrupt buckets: {scan:?}");
        assert_eq!(report.verdict, Verdict::Valid);

        // A bare data file with no sidecar dirs validates data-only (index_scan None), still Valid.
        {
            let bare = TempDir::with_prefix("test_validate_bare").expect("temp dir");
            let bare_data = bare.path().join(Inner::DATA_NAME);
            std::fs::copy(&data_path, &bare_data).expect("copy data file");
            let report = validate_pack_file(&bare_data, 0, None).expect("validate bare data file");
            assert!(report.index_scan.is_none(), "no sidecar dirs -> data-only validation");
            assert_eq!(report.verdict, Verdict::Valid);
        }

        // Flip a byte in the last bucket's payload, leaving its (non-zero) stamped CRC in place ->
        // a corrupt bucket the scan must surface.
        {
            let mut bytes = std::fs::read(&hdx_path).expect("read hdx");
            let n = bytes.len();
            bytes[n - HDX_BUCKET + 12] ^= 0xFF; // payload region, not the trailing 4-byte CRC
            std::fs::write(&hdx_path, &bytes).expect("write hdx");
        }
        let report = validate_pack_file(&data_path, 0, None).expect("validate corrupt bucket");
        let scan = report.index_scan.expect("scanned");
        assert!(scan.consensus.corrupt >= 1, "a payload flip must report corrupt: {scan:?}");
        assert_eq!(report.verdict, Verdict::Invalid, "a degraded index makes the pack Invalid");

        // Zero the whole last bucket (payload + CRC): the launder-prone lost-page shape -> dirty.
        {
            let mut bytes = std::fs::read(&hdx_path).expect("read hdx");
            let n = bytes.len();
            bytes[n - HDX_BUCKET..].fill(0);
            std::fs::write(&hdx_path, &bytes).expect("write hdx");
        }
        let report = validate_pack_file(&data_path, 0, None).expect("validate zeroed bucket");
        let scan = report.index_scan.expect("scanned");
        assert!(scan.consensus.dirty >= 1, "a zeroed bucket must report dirty: {scan:?}");
        assert_eq!(report.verdict, Verdict::Invalid);
    }

    /// A crash between the data-header write and the first epoch-meta append leaves the file grown
    /// to its mmap capacity and zero-padded, with no clean-close sentinel and no meta record.
    /// `open_append` must treat that as header-only and (re)initialize the meta — dropping the
    /// padding so the meta lands at `DATA_HEADER_BYTES` — rather than mistaking the zero padding
    /// for a torn meta and failing fatally (which would strand a fresh node on first boot).
    #[tokio::test]
    async fn test_open_append_reinitializes_meta_after_crash_before_meta_write() {
        let temp_dir = TempDir::with_prefix("test_cp_crash_before_meta").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Seed a valid epoch-0 data header by cleanly creating (and closing) a real pack.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("seed pack");
            pack.persist().await.expect("persist");
        }

        // Reconstruct the exact on-disk state a crash-before-meta leaves: the 28-byte header
        // followed by zero padding (the grown mmap capacity), with no clean-close sentinel and no
        // meta record. Keeping only the header discards the meta the seed pack wrote.
        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        let mut padded =
            std::fs::read(&data_path).expect("read seed data")[..DATA_HEADER_BYTES].to_vec();
        padded.resize(DATA_HEADER_BYTES + 4096, 0);
        std::fs::write(&data_path, &padded).expect("write padded header-only file");

        // Must not fail: the padding is not a torn meta. The meta is reinitialized and the pack is
        // usable again.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("open_append must reinitialize the meta, not reject the padding");
            let output = make_test_output(
                &committee,
                0,
                chain.clone(),
                1,
                ConsensusHeader::default().digest(),
            );
            pack.save_consensus_output(output).await.expect("save output after reinit");
            pack.persist().await.expect("persist");
        }

        // The reinitialized pack is consistent and serves the output through the read-only door.
        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after reinit");
        assert!(
            pack.get_consensus_output(1).await.is_ok(),
            "output must read back after the meta was reinitialized"
        );
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

    /// R2 regression: zeroing the epoch meta's 4-byte length prefix on an *occupied* pack must not
    /// be mistaken for a header-only file and re-initialized -- that would silently erase the meta
    /// payload and every committed output. `open_append` must fail closed (like
    /// `open_append_exists` and the torn-prefix path) and leave the data untouched. The
    /// existing meta-corruption tests use a damaged payload or an inflated, *nonzero* prefix,
    /// so they take the rejecting `if` branch and miss this zeroed-prefix `else`-branch case.
    #[tokio::test]
    async fn test_zero_meta_length_preserves_existing_outputs() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        let temp_dir = TempDir::with_prefix("test_cp_zero_meta_len").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack(&temp_dir, &committee, &chain, &previous_epoch, 3).await;

        let data_path = temp_dir.path().join("epoch-0").join(Inner::DATA_NAME);
        // All three outputs + meta + clean-close sentinel; must survive the rejected open.
        let committed_len = std::fs::metadata(&data_path).expect("metadata").len();

        // Zero ONLY the meta record's 4-byte length prefix at DATA_HEADER_BYTES, leaving the meta
        // payload, all outputs, the indexes, and the clean-close sentinel intact.
        // `record_present_at` now reads the prefix as empty even though the pack is fully
        // occupied.
        {
            let mut f =
                OpenOptions::new().read(true).write(true).open(&data_path).expect("open data");
            f.seek(SeekFrom::Start(DATA_HEADER_BYTES as u64)).expect("seek");
            f.write_all(&[0u8; 4]).expect("zero the meta length prefix");
            f.sync_all().expect("sync");
        }

        // Must reject (not re-initialize), and must leave the occupied data untouched.
        let res =
            ConsensusPack::open_append(temp_dir.path(), previous_epoch.clone(), committee.clone());
        assert!(
            matches!(res, Err(super::PackError::EpochLoad(_))),
            "a zeroed meta length prefix over committed data must reject, got {res:?}"
        );
        assert_eq!(
            std::fs::metadata(&data_path).expect("metadata").len(),
            committed_len,
            "the rejected open must not truncate the occupied pack (outputs preserved)"
        );
    }

    /// An unclean stop while an epoch's first output is in flight can leave the position index
    /// holding that output's lone entry while the data log ends at the epoch meta or inside the
    /// output. Recovery rebuilds the index from the data log, so the lone entry must not survive
    /// any of these shapes: the pack reports `start - 1` as its latest number, has no latest
    /// header, and accepts the output again.
    #[tokio::test]
    async fn test_recover_drops_lone_index_entry_for_torn_first_output() {
        use crate::{
            archive::{
                data_file::SENTINEL_LEN, pack::DataHeader, position_index::index::PositionIndex,
            },
            consensus_pack::IndexPositions,
        };
        use std::io::Write as _;

        /// Where the data log ends relative to output 1 when the pack reopens.
        #[derive(Debug, Clone, Copy)]
        enum TornFirstOutput {
            /// Cut back to the end of the epoch meta after a clean close.
            CutToMeta,
            /// Cut one byte short of the output's end after a clean close.
            CutInsideOutput,
            /// A power loss before the output's persist: the index entry reached disk and the
            /// output's data did not.
            PowerLoss,
        }

        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let output =
            make_test_output(&committee, 0, chain.clone(), 1, ConsensusHeader::default().digest());

        for case in [
            TornFirstOutput::CutToMeta,
            TornFirstOutput::CutInsideOutput,
            TornFirstOutput::PowerLoss,
        ] {
            let temp_dir = TempDir::with_prefix("test_cp_lone_torn_entry").expect("temp dir");
            let epoch_dir = temp_dir.path().join("epoch-0");
            let data_path = epoch_dir.join(Inner::DATA_NAME);
            // lengths come from the pack's logical end: while a pack is open the physical length
            // is the mmap capacity
            let (meta_end, output_end) = match case {
                TornFirstOutput::CutToMeta | TornFirstOutput::CutInsideOutput => {
                    let pack = ConsensusPack::open_append(
                        temp_dir.path(),
                        previous_epoch.clone(),
                        committee.clone(),
                    )
                    .expect("open pack");
                    pack.persist().await.expect("persist meta");
                    let meta_end = pack.data_file_len().await.expect("meta end");
                    pack.save_consensus_output(output.clone()).await.expect("save output");
                    let output_end = pack.data_file_len().await.expect("output end");
                    pack.persist().await.expect("persist output");
                    pack.close().await;
                    // a clean close trims the log to its logical end plus the clean-close sentinel
                    let full_len = std::fs::metadata(&data_path).expect("metadata").len();
                    assert_eq!(full_len, output_end + SENTINEL_LEN, "{case:?}: sealed log length");
                    let cut_len = if matches!(case, TornFirstOutput::CutToMeta) {
                        meta_end
                    } else {
                        full_len - SENTINEL_LEN - 1
                    };
                    let f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&data_path)
                        .expect("open data");
                    f.set_len(cut_len).expect("truncate");
                    (meta_end, output_end)
                }
                TornFirstOutput::PowerLoss => {
                    let mut inner = Inner::open_append(
                        temp_dir.path(),
                        &previous_epoch,
                        committee.clone(),
                        PACK_VERSION,
                    )
                    .expect("open_append inner");
                    inner.persist().expect("persist meta");
                    let meta_end = inner.data.file_len();
                    inner.save_consensus_output(&output).expect("save output");
                    let output_end = inner.data.file_len();
                    // the process dies before the output's persist and before any clean close
                    std::mem::forget(inner);
                    // writeback order is arbitrary, so the index page can reach disk while the
                    // output's data pages do not
                    let mut f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&data_path)
                        .expect("open data");
                    f.seek(SeekFrom::Start(meta_end)).expect("seek");
                    f.write_all(&vec![0u8; (output_end - meta_end) as usize])
                        .expect("zero the output");
                    (meta_end, output_end)
                }
            };

            // the on-disk index holds exactly the lone entry for output 1. zero padding past an
            // unsealed index decodes as zero entries, which never point past the meta.
            let header =
                DataHeader::load_header(&mut File::open(&data_path).expect("open data"), 0)
                    .expect("data header");
            let pdx_path =
                epoch_dir.join(Inner::CONSENSUS_POS_NAME).join(Inner::CONSENSUS_POS_FILE);
            let entries: Vec<_> = PositionIndex::<IndexPositions>::raw_entries(&pdx_path, &header)
                .into_iter()
                .filter(|p| p.consensus_header >= meta_end)
                .map(|p| (p.consensus_header, p.output_start, p.output_end))
                .collect();
            assert_eq!(
                entries,
                vec![(meta_end, meta_end, output_end)],
                "{case:?}: the lone index entry must be on disk before the reopen"
            );

            let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
                .unwrap_or_else(|e| panic!("{case:?}: the open must recover the pack: {e:?}"));
            // epoch 0 starts at number 1, so a pack without outputs reports 0
            assert_eq!(
                pack.latest_consensus_number().await.expect("latest number"),
                0,
                "{case:?}: the lone torn index entry must be dropped"
            );
            let latest = pack.latest_consensus_header().await;
            assert!(matches!(latest, Ok(None)), "{case:?}: expected Ok(None), got {latest:?}");
            assert_eq!(
                pack.data_file_len().await.expect("data len"),
                meta_end,
                "{case:?}: recovery must rewind the log to the meta"
            );
            pack.save_consensus_output(output.clone())
                .await
                .unwrap_or_else(|e| panic!("{case:?}: the dropped output must be accepted: {e:?}"));
            pack.persist().await.expect("persist after recovery");
            pack.close().await;

            let pack = ConsensusPack::open_append_exists(temp_dir.path(), 0)
                .unwrap_or_else(|e| panic!("{case:?}: reopen after the save: {e:?}"));
            assert_eq!(
                pack.latest_consensus_number().await.expect("latest number"),
                1,
                "{case:?}: the saved output is the new tail"
            );
            let latest =
                pack.latest_consensus_header().await.expect("read latest").expect("latest header");
            assert_eq!(latest.number, 1, "{case:?}: latest header number");
            assert_eq!(
                pack.data_file_len().await.expect("data len"),
                output_end,
                "{case:?}: exactly one copy of output 1 follows the meta"
            );
            pack.close().await;
        }
    }

    /// The header-only branch reinitializes the digest index lengths: the data file is rolled back
    /// to exactly `DATA_HEADER_BYTES` while the digest indexes keep their larger pre-damage length.
    /// That branch writes a fresh meta, so it needs the index length reinitialized -- without
    /// `recover_pack` re-stamping `set_data_file_length`, recovery cuts the new meta down to the
    /// stale length and hands back a live pack whose first record is torn.
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
        // complete output, so the logical data ends at output 2; the clean close then re-appends
        // the 8-byte sentinel.
        let recovered_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert_eq!(
            recovered_len,
            output2_end + crate::archive::data_file::SENTINEL_LEN,
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
            DATA_HEADER_BYTES as u64 + crate::archive::data_file::SENTINEL_LEN,
            "setup must produce a header-only data file (plus the clean-close sentinel)"
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

    /// The header-only recovery must also handle a *padded* header-only file: a crash right after
    /// pack creation can leave the data file grown to its mmap capacity (zero-padded past the
    /// 28-byte header) with no clean-close sentinel -- unlike the exactly-header-sized file the
    /// sibling test uses. `open_append` must roll the logical end back to the header (via
    /// `rewind_to`) and append the meta, rather than mistake the padding for a torn record. This is
    /// the only test that reaches the `pack_len > DATA_HEADER_BYTES` trim in the header-only
    /// branch.
    #[tokio::test]
    async fn test_open_append_recovers_padded_header_only_file() {
        let temp_dir = TempDir::with_prefix("test_cp_padded_header_only").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Write the data header, then LEAK the pack (skip the clean-close Drop) so the file keeps
        // its mmap capacity padding and gets no sentinel -- the on-disk shape of a crash between
        // the header write and the first (meta) append.
        let base_dir = temp_dir.path().join("epoch-0");
        std::fs::create_dir_all(&base_dir).expect("create epoch dir");
        let data_path = base_dir.join(Inner::DATA_NAME);
        {
            let mut raw: Pack<PackRecord> =
                Pack::open(&data_path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("raw pack");
            raw.commit().expect("commit header");
            std::mem::forget(raw); // no clean close: no truncate, no sentinel
        }
        let padded_len = std::fs::metadata(&data_path).expect("metadata").len();
        assert!(
            padded_len > DATA_HEADER_BYTES as u64 + crate::archive::data_file::SENTINEL_LEN,
            "setup must leave a padded, unsentineled header-only file (got {padded_len} bytes)"
        );

        // open_append must reach the header-only branch, roll the padding back, and append the
        // meta.
        {
            let pack = ConsensusPack::open_append(
                temp_dir.path(),
                previous_epoch.clone(),
                committee.clone(),
            )
            .expect("padded header-only file must recover");
            let parent = ConsensusHeader::default().digest();
            let output = make_test_output(&committee, 0, chain.clone(), 1, parent);
            pack.save_consensus_output(output).await.unwrap();
            pack.persist().await.expect("persist");
        }

        // Reopen read-only: the meta + output are durable and consistent.
        let pack =
            ConsensusPack::open_static(temp_dir.path(), 0).expect("open static after recovery");
        assert_eq!(
            pack.get_consensus_output(1).await.expect("recovered output").number(),
            1,
            "static read must return the recovered output",
        );
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

    /// An epoch-1 committee and the epoch-0 record that hands off to it, with non-default genesis
    /// links so every authenticated [`EpochMeta`] field has a value to drift from.
    fn open_append_meta_fixture() -> (Committee, EpochRecord) {
        use std::collections::BTreeMap;

        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{Address, Authority, BlockNumHash, BlsKeypair, BlsPublicKey};

        let mut rng = StdRng::seed_from_u64(0x0BE7A);
        let keys: Vec<BlsPublicKey> =
            (0..4).map(|_| *BlsKeypair::generate(&mut rng).public()).collect();
        let authorities = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, Authority::new_for_test(*k, Address::repeat_byte(i as u8 + 1))))
            .collect::<BTreeMap<_, _>>();
        let committee = Committee::new_for_test(authorities, 1, BTreeMap::default());
        let previous = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys,
            final_state: BlockNumHash::new(42, BlockHash::repeat_byte(0x42)),
            final_consensus: ConsensusNumHash::new(10, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        (committee, previous)
    }

    /// The meta `Inner::open_append` derives for `committee`'s epoch from the `previous` record:
    /// the first record of a pack this node creates, and what a reopen compares the on-disk meta
    /// against.
    fn derived_epoch_meta(committee: &Committee, previous: &EpochRecord) -> EpochMeta {
        let epoch = committee.epoch();
        EpochMeta {
            epoch,
            committee: committee.clone(),
            start_consensus_number: epoch_start_consensus_number(epoch, previous),
            genesis_exec_state: previous.final_state,
            genesis_consensus: previous.final_consensus,
        }
    }

    /// `committee` with fields outside its BLS key set changed: every execution address and, where
    /// the multi-worker fork is active for its epoch, the worker count. A pack imported from a peer
    /// can carry such a committee, because import authenticates only the key set. Before the fork
    /// (adiri builds) the committee layout encodes a single worker, so only the addresses drift.
    fn with_unauthenticated_drift(committee: &Committee) -> Committee {
        use tn_types::{forks::multi_workers_fork_active, Address, Authority};

        let authorities = committee
            .authorities()
            .into_iter()
            .map(|authority| {
                let key = *authority.protocol_key();
                (key, Authority::new_for_test(key, Address::repeat_byte(0xEE)))
            })
            .collect();
        // the pre-fork layout refuses to encode any worker count but one
        let added = if multi_workers_fork_active(committee.epoch()) { 2 } else { 0 };
        let workers = NonZeroUsize::new(committee.number_of_workers() + added)
            .expect("a positive worker count");
        let drifted =
            Committee::new_for_test(authorities, committee.epoch(), committee.bootstrap_servers())
                .with_num_workers(workers);
        assert_eq!(drifted.bls_keys(), committee.bls_keys(), "drift must keep the BLS key set");
        assert_ne!(&drifted, committee, "drift must change the committee");
        drifted
    }

    /// Writes `meta` as the only record of a fresh data file for `epoch` under `dir`, standing in
    /// for a pack whose meta this node did not write itself (an import), and returns the data
    /// file's path. `epoch` places and stamps the file, so `meta` may claim a different one.
    fn write_epoch_meta_by_hand(dir: &Path, epoch: Epoch, meta: &EpochMeta) -> std::path::PathBuf {
        let epoch_dir = dir.join(format!("epoch-{epoch}"));
        std::fs::create_dir_all(&epoch_dir).expect("create epoch dir");
        let data_path = epoch_dir.join(Inner::DATA_NAME);
        let mut pack: Pack<PackRecord> =
            Pack::open(&data_path, epoch as u64, false, PackCompression::ZStd, PACK_VERSION)
                .expect("open data file");
        pack.append(&PackRecord::EpochMeta(meta.clone())).expect("append meta");
        pack.commit().expect("commit meta");
        data_path
    }

    /// Reads the epoch meta at the head of the data file at `data_path`.
    fn read_epoch_meta(data_path: &Path, epoch: Epoch) -> EpochMeta {
        let mut pack: Pack<PackRecord> =
            Pack::open(data_path, epoch as u64, true, PackCompression::ZStd, PACK_VERSION)
                .expect("open data file read-only");
        pack.fetch(DATA_HEADER_BYTES as u64)
            .expect("fetch the first record")
            .into_epoch()
            .expect("the first record is the epoch meta")
    }

    /// `open_append` reopens an epoch whose on-disk meta matches the chain-derived meta on every
    /// field import authenticates but carries a committee that differs outside its key set, as a
    /// pack imported from a peer can. The pack carries the chain-derived committee and the on-disk
    /// meta stays as it was.
    #[tokio::test]
    async fn test_open_append_accepts_unauthenticated_committee_drift() {
        use crate::consensus_pack::verify_epoch_meta;

        let temp_dir = TempDir::with_prefix("test_cp_meta_unauth_drift").expect("temp dir");
        let (committee, previous) = open_append_meta_fixture();
        let on_disk = EpochMeta {
            committee: with_unauthenticated_drift(&committee),
            ..derived_epoch_meta(&committee, &previous)
        };
        verify_epoch_meta(1, &previous, &on_disk).expect("import must accept the drifted meta");
        let data_path = write_epoch_meta_by_hand(temp_dir.path(), 1, &on_disk);

        let pack = ConsensusPack::open_append(temp_dir.path(), previous, committee.clone())
            .expect("a committee drifted outside its BLS key set must not refuse the reopen");
        assert_eq!(pack.committee(), &committee, "the pack must carry the chain-derived committee");
        pack.close().await;

        assert_eq!(
            read_epoch_meta(&data_path, 1),
            on_disk,
            "the reopen must not rewrite the on-disk meta"
        );
    }

    /// `Inner::open_append` over an on-disk meta drifted outside the authenticated fields keeps
    /// the meta it derives from the chain, not the one it read. The [`ConsensusPack`] wrapper
    /// stores the caller's committee either way, so only the inner meta shows which one was kept.
    #[test]
    fn test_inner_open_append_keeps_chain_derived_meta() {
        let temp_dir = TempDir::with_prefix("test_cp_meta_inner_keeps").expect("temp dir");
        let (committee, previous) = open_append_meta_fixture();
        let derived = derived_epoch_meta(&committee, &previous);
        let on_disk =
            EpochMeta { committee: with_unauthenticated_drift(&committee), ..derived.clone() };
        write_epoch_meta_by_hand(temp_dir.path(), 1, &on_disk);

        let inner = Inner::open_append(temp_dir.path(), &previous, committee, PACK_VERSION)
            .expect("a committee drifted outside its BLS key set must not refuse the reopen");
        assert_eq!(inner.epoch_meta, derived, "the open must keep the chain-derived meta");
        assert_ne!(inner.epoch_meta, on_disk, "the on-disk meta must not replace it");
    }

    /// `open_append` refuses to reopen an epoch whose on-disk meta differs from the chain-derived
    /// meta on any field import authenticates. The error names the differing field, and the
    /// refusal comes before any write: the epoch directory is left byte for byte as it was.
    #[tokio::test]
    async fn test_open_append_rejects_authenticated_meta_drift() {
        use std::collections::BTreeMap;

        use tn_types::BlockNumHash;

        use crate::consensus_pack::PackError;

        let (committee, previous) = open_append_meta_fixture();
        let local = derived_epoch_meta(&committee, &previous);
        let one_dropped = Committee::new_for_test(
            committee.authorities().into_iter().skip(1).map(|a| (*a.protocol_key(), a)).collect(),
            committee.epoch(),
            BTreeMap::default(),
        );
        let drifts = [
            ("epoch", EpochMeta { epoch: 2, ..local.clone() }),
            (
                "committee epoch",
                EpochMeta { committee: committee.advance_epoch_for_test(2), ..local.clone() },
            ),
            ("committee bls keys", EpochMeta { committee: one_dropped, ..local.clone() }),
            (
                "genesis_consensus",
                EpochMeta {
                    genesis_consensus: ConsensusNumHash::new(
                        local.genesis_consensus.number,
                        ConsensusHeader::default().digest(),
                    ),
                    ..local.clone()
                },
            ),
            (
                "start_consensus_number",
                EpochMeta {
                    start_consensus_number: local.start_consensus_number + 1,
                    ..local.clone()
                },
            ),
            (
                "genesis_exec_state",
                EpochMeta {
                    genesis_exec_state: BlockNumHash::new(
                        local.genesis_exec_state.number,
                        BlockHash::repeat_byte(0x24),
                    ),
                    ..local.clone()
                },
            ),
        ];
        // every entry of the epoch directory, with each file's bytes
        let snapshot = |dir: &Path| {
            std::fs::read_dir(dir)
                .expect("read epoch dir")
                .map(|entry| {
                    let path = entry.expect("epoch dir entry").path();
                    let bytes = path.is_file().then(|| std::fs::read(&path).expect("read file"));
                    (path, bytes)
                })
                .collect::<BTreeMap<_, _>>()
        };

        for (field, on_disk) in drifts {
            assert_ne!(on_disk, local, "{field}: the case must drift");
            let temp_dir = TempDir::with_prefix("test_cp_meta_auth_drift").expect("temp dir");
            let data_path = write_epoch_meta_by_hand(temp_dir.path(), 1, &on_disk);
            let epoch_dir = data_path.parent().expect("data file is in its epoch dir");
            let before = snapshot(epoch_dir);

            let Err(err) =
                ConsensusPack::open_append(temp_dir.path(), previous.clone(), committee.clone())
            else {
                panic!("{field}: a drifted authenticated field must refuse the reopen")
            };
            let PackError::InvalidEpoch(1, msg) = &err else {
                panic!("{field}: expected InvalidEpoch(1, _), got {err:?}")
            };
            assert!(
                msg.starts_with(&format!("open append has unexpected meta data: {field}:")),
                "{field}: the error must name the field: {msg}"
            );
            assert_eq!(
                snapshot(epoch_dir),
                before,
                "{field}: a refused reopen must leave the epoch directory unchanged"
            );
        }
    }

    /// Over the ejection shapes of `test_verify_epoch_meta_across_ejection_shapes`, `open_append`'s
    /// reopen check accepts an imported meta exactly when `verify_epoch_meta` does, for a node
    /// whose chain-derived committee holds the key set import authenticates against
    /// (`previous.next_committee`) but different addresses (and, past the multi-worker fork,
    /// worker count). Two shapes are accepted and two refused, so it fails if
    /// `authenticated_mismatch` compares a field import does not authenticate or stops comparing
    /// the key set.
    #[test]
    fn test_open_append_check_is_implied_by_verify_epoch_meta() {
        use std::collections::BTreeMap;

        use rand::{rngs::StdRng, SeedableRng as _};
        use tn_types::{Address, Authority, BlsKeypair, BlsPublicKey};

        use crate::consensus_pack::verify_epoch_meta;

        let mut rng = StdRng::seed_from_u64(0xE2EC7);
        let keys: Vec<BlsPublicKey> =
            (0..5).map(|_| *BlsKeypair::generate(&mut rng).public()).collect();
        let build_committee = |members: &[BlsPublicKey], epoch: Epoch| {
            let authorities = members
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, Authority::new_for_test(*k, Address::repeat_byte(i as u8 + 1))))
                .collect::<BTreeMap<_, _>>();
            Committee::new_for_test(authorities, epoch, BTreeMap::default())
        };

        // swap-and-pop ejection of keys[2]: rec0 predates it (next committee stored in reverse
        // order), rec1 is the ejection epoch's record
        let survivors = vec![keys[0], keys[1], keys[4], keys[3]];
        let mut next0 = keys.clone();
        next0.reverse();
        let rec0 = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: next0,
            final_consensus: ConsensusNumHash::new(10, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        let rec1 = EpochRecord {
            epoch: 1,
            committee: survivors.clone(),
            next_committee: survivors.clone(),
            final_consensus: ConsensusNumHash::new(20, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        let shapes = [
            (&rec0, build_committee(&keys, 1)),
            (&rec0, build_committee(&survivors, 1)),
            (&rec1, build_committee(&survivors, 2)),
            (&rec1, build_committee(&keys, 2)),
        ];

        let mut accepted = 0;
        for (previous, imported_committee) in shapes {
            let imported = derived_epoch_meta(&imported_committee, previous);
            let epoch = imported.epoch;
            let local_committee =
                with_unauthenticated_drift(&build_committee(&previous.next_committee, epoch));
            let local = derived_epoch_meta(&local_committee, previous);
            let import_accepts = verify_epoch_meta(epoch, previous, &imported).is_ok();
            assert_eq!(
                local.authenticated_mismatch(&imported).is_none(),
                import_accepts,
                "epoch {epoch}: open_append must reopen exactly the metas import accepts"
            );
            accepted += usize::from(import_accepts);
        }
        assert_eq!(accepted, 2, "two of the ejection shapes are metas import accepts");
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

    /// Finding #6: a decoded consensus header whose sub-dag has no headers (hence no leader) must
    /// be rejected at the decode chokepoint, not turned into an output that panics the moment
    /// any `leader()`-derived accessor is touched. `bytes_to_output` is the per-output decode
    /// path used to serve/receive a single output (`request_consensus_output`). Without the
    /// guard this returns `Ok` with a leaderless output that later panics; with it the decode
    /// fails cleanly.
    #[tokio::test]
    async fn test_bytes_to_output_rejects_empty_subdag() {
        use crate::{
            archive::pack::{Pack, DATA_HEADER_BYTES},
            consensus_pack::{bytes_to_output, PackError, PackRecord},
        };
        use std::io::Cursor;

        let temp_dir = TempDir::with_prefix("test_cp_empty_subdag").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();

        // A well-framed header carrying an empty sub-dag: decodable, but leaderless.
        let header = ConsensusHeader {
            parent_hash: Default::default(),
            sub_dag: CommittedSubDag::new_with_headers_for_test(vec![]),
            number: 1,
            extra: Default::default(),
        };
        assert!(header.sub_dag.is_empty(), "the crafted sub-dag must be empty");

        let path = temp_dir.path().join("empty_subdag");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            pack.append(&PackRecord::Consensus(Box::new(header))).expect("append header");
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
        assert!(
            matches!(res, Err(PackError::EmptySubDag)),
            "an empty sub-dag must be rejected as EmptySubDag, got {res:?}"
        );
    }

    /// Finding #6: the same empty sub-dag arriving over an epoch-sync stream must not panic the
    /// (critical) import task. `stream_import` decodes each output through the same chokepoint, so
    /// a hostile empty sub-dag is rejected with `EmptySubDag` before `save_consensus_output`
    /// calls `leader_epoch()`. The output's parent link is the expected genesis parent, so
    /// without the guard the import reaches `leader_epoch()` and this test panics instead of
    /// erroring.
    #[tokio::test]
    async fn test_stream_import_rejects_empty_subdag() {
        use crate::{
            archive::pack::Pack,
            consensus_pack::{EpochMeta, PackError, PackRecord},
        };

        let temp_dir = TempDir::with_prefix("test_cp_import_empty_subdag").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        let source = temp_dir.path().join("peer_stream");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&source, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open peer stream");
            pack.append(&PackRecord::EpochMeta(EpochMeta {
                epoch: 0,
                committee: committee.clone(),
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            }))
            .expect("append meta");
            pack.append(&PackRecord::Consensus(Box::new(ConsensusHeader {
                parent_hash: previous_epoch.final_consensus.hash,
                sub_dag: CommittedSubDag::new_with_headers_for_test(vec![]),
                number: 1,
                extra: Default::default(),
            })))
            .expect("append empty-sub-dag output");
            pack.commit().expect("commit peer stream");
        }

        let target = TempDir::with_prefix("test_cp_import_empty_subdag_out").expect("temp dir");
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
        .expect_err("an empty sub-dag must be rejected, not imported");
        assert!(matches!(err, PackError::EmptySubDag), "got {err:?}");
    }

    /// A stream import stops before it drives the filesystem below its free-space floor, with a
    /// local `StorageFull` I/O error (never charged to the peer), and checks again each time the
    /// data log has grown by the check interval.
    #[tokio::test]
    async fn test_stream_import_stops_at_the_free_space_floor() {
        use std::io;

        use crate::consensus_pack::{DiskFloor, PackError, IMPORT_FREE_CHECK_EVERY};
        let temp_dir = TempDir::with_prefix("test_import_floor").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let previous_epoch = test_previous_epoch(&fixture.committee());
        let is_storage_full = |e: &PackError| matches!(e, PackError::IO(io_err) if io_err.kind() == io::ErrorKind::StorageFull);

        let Err(err) = Inner::stream_import(
            temp_dir.path(),
            &[][..],
            0,
            &previous_epoch,
            1,
            Duration::from_secs(5),
            u64::MAX,
        )
        .await
        else {
            panic!("an import with no room must stop");
        };
        assert!(is_storage_full(&err), "got {err:?}");

        let mut floor =
            DiskFloor { dir: temp_dir.path().to_owned(), min_free: u64::MAX, checked_at: Some(0) };
        assert!(floor.check(IMPORT_FREE_CHECK_EVERY - 1).is_ok(), "no check within the interval");
        let err = floor.check(IMPORT_FREE_CHECK_EVERY).expect_err("checked after the interval");
        assert!(is_storage_full(&err), "got {err:?}");
    }

    /// A streamed import builds a fresh pack strictly in order, so a header whose number does not
    /// advance must be rejected — not accepted-and-ignored. Here two outputs share number 1 with a
    /// valid parent link, so only the number is wrong. Without the advancement check the second is
    /// a silent no-op (`save_consensus_output`'s idempotent `idx < len` path) and an endless
    /// such chain pins the import forever; with it, the second output is
    /// `InvalidConsensusNumber`, so this returns `Err` instead of `Ok`. (Over the peer-import
    /// sync path the requester then charges the peer a Severe penalty.)
    #[tokio::test]
    async fn test_stream_import_rejects_non_advancing_number() {
        use crate::{
            archive::pack::Pack,
            consensus_pack::{EpochMeta, PackError, PackRecord},
        };

        let temp_dir = TempDir::with_prefix("test_cp_non_advancing").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);

        // Two batch-less outputs (a leader header, no payload) that both claim number 1. The
        // second's parent link is the first's digest, so the parent-chain check passes and
        // ONLY the number is wrong.
        let leader_header = Certificate::default().header().clone();
        let header1 = ConsensusHeader {
            parent_hash: previous_epoch.final_consensus.hash,
            sub_dag: CommittedSubDag::new_with_headers_for_test(vec![leader_header.clone()]),
            number: 1,
            extra: Default::default(),
        };
        let header2 = ConsensusHeader {
            parent_hash: header1.digest(),
            sub_dag: CommittedSubDag::new_with_headers_for_test(vec![leader_header]),
            number: 1,
            extra: Default::default(),
        };

        let source = temp_dir.path().join("peer_stream");
        {
            let mut pack: Pack<PackRecord> =
                Pack::open(&source, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open peer stream");
            pack.append(&PackRecord::EpochMeta(EpochMeta {
                epoch: 0,
                committee: committee.clone(),
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            }))
            .expect("append meta");
            pack.append(&PackRecord::Consensus(Box::new(header1))).expect("append output 1");
            pack.append(&PackRecord::Consensus(Box::new(header2))).expect("append repeat output");
            pack.commit().expect("commit peer stream");
        }

        let target = TempDir::with_prefix("test_cp_non_advancing_out").expect("temp dir");
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        // A final well past output 1, so the import keeps reading after it (it stops at the final).
        let err = ConsensusPack::stream_import(
            target.path(),
            stream,
            0,
            &previous_epoch,
            5,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a non-advancing consensus number must be rejected, not accepted-and-ignored");
        assert!(matches!(err, PackError::InvalidConsensusNumber(2, 1)), "got {err:?}");
    }

    /// A number that is not the next one is a gap whatever its value: a peer that jumps past
    /// the requester's final is charged like any other gap (`InvalidConsensusNumber`, Severe),
    /// not excused as the requester's stale final (`ConsensusNumberTooHigh`, no penalty). The
    /// stale-final case is still recognised: the NEXT number lying past the final.
    #[tokio::test]
    async fn test_stream_import_charges_a_gap_past_the_final_as_a_gap() {
        use crate::{
            archive::pack::Pack,
            consensus_pack::{EpochMeta, PackError, PackRecord},
        };

        let temp_dir = TempDir::with_prefix("test_cp_gap_past_final").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let leader_header = Certificate::default().header().clone();
        let header1 = ConsensusHeader {
            parent_hash: previous_epoch.final_consensus.hash,
            sub_dag: CommittedSubDag::new_with_headers_for_test(vec![leader_header.clone()]),
            number: 1,
            extra: Default::default(),
        };
        let jumped = ConsensusHeader {
            parent_hash: header1.digest(),
            sub_dag: CommittedSubDag::new_with_headers_for_test(vec![leader_header]),
            number: u64::MAX,
            extra: Default::default(),
        };
        let write_stream = |path: &Path, headers: &[&ConsensusHeader]| {
            let mut pack: Pack<PackRecord> =
                Pack::open(path, 0, false, PackCompression::ZStd, PACK_VERSION)
                    .expect("open peer stream");
            pack.append(&PackRecord::EpochMeta(EpochMeta {
                epoch: 0,
                committee: committee.clone(),
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            }))
            .expect("append meta");
            for header in headers {
                pack.append(&PackRecord::Consensus(Box::new((*header).clone())))
                    .expect("append output");
            }
            pack.commit().expect("commit peer stream");
        };

        // A jump past the final at the second slot: a gap, charged as one.
        let source = temp_dir.path().join("gap_stream");
        write_stream(&source, &[&header1, &jumped]);
        let target = TempDir::with_prefix("test_cp_gap_out").expect("temp dir");
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        let err = ConsensusPack::stream_import(
            target.path(),
            stream,
            0,
            &previous_epoch,
            5,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a gap past the final is still a gap");
        assert!(matches!(err, PackError::InvalidConsensusNumber(2, u64::MAX)), "got {err:?}");

        // The next number past a stale local final (a dummy `0`): the requester's problem.
        let source = temp_dir.path().join("stale_final_stream");
        write_stream(&source, &[&header1]);
        let target = TempDir::with_prefix("test_cp_stale_out").expect("temp dir");
        let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
        let err = ConsensusPack::stream_import(
            target.path(),
            stream,
            0,
            &previous_epoch,
            0,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the next number past a stale final stops the import");
        assert!(matches!(err, PackError::ConsensusNumberTooHigh), "got {err:?}");
    }

    /// Stream the logical bytes (`[0, end)`, without the clean-close sentinel) of the sealed
    /// epoch-0 pack under `dir`, the way a peer serves an epoch.
    async fn epoch0_pack_stream(dir: &Path) -> impl tokio::io::AsyncRead + Unpin {
        use tokio::io::AsyncReadExt as _;
        let data_file = dir.join("epoch-0").join(Inner::DATA_NAME);
        let logical_len = std::fs::metadata(&data_file).expect("meta").len()
            - crate::archive::data_file::SENTINEL_LEN;
        tokio::fs::File::open(&data_file).await.expect("open pack data").take(logical_len)
    }

    /// An imported epoch is written in the current format: a v1 (header-first) peer stream lands
    /// on disk as a v2 pack, so no read of it later has to migrate it, and every output
    /// round-trips unchanged. A v0 (batches-first) source is refused before anything is written —
    /// v0 is only ever migrated on disk — as `InvalidVersion`, which charges the peer no penalty
    /// (an older build's honest bytes).
    #[tokio::test]
    async fn test_stream_import_writes_v2_and_refuses_a_v0_source() {
        for version in [1_u16, PACK_VERSION] {
            let source = TempDir::with_prefix("test_import_v2_src").expect("temp dir");
            let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let committee = fixture.committee();
            let previous_epoch = test_previous_epoch(&committee);
            let outputs =
                build_test_pack_version(&source, &committee, &chain, &previous_epoch, 4, version)
                    .await;

            let target = TempDir::with_prefix("test_import_v2_dst").expect("temp dir");
            let imported = ConsensusPack::stream_import(
                target.path(),
                epoch0_pack_stream(source.path()).await,
                0,
                &previous_epoch,
                4,
                Duration::from_secs(5),
            )
            .await
            .unwrap_or_else(|e| panic!("v{version} source must import, got {e:?}"));
            assert_eq!(imported.version, PACK_VERSION, "v{version} source: handle not v2");
            for expected in &outputs {
                let got = imported.get_consensus_output(expected.number()).await.expect("imported");
                compare_outputs(&got, expected);
            }
            imported.close().await;

            let data_path = target.path().join("epoch-0").join(Inner::DATA_NAME);
            assert_eq!(
                peek_pack_version(&data_path),
                PACK_VERSION,
                "v{version} source: the imported data file must be v2 on disk"
            );
            assert!(
                !ConsensusPack::epoch_is_legacy(target.path(), 0),
                "v{version} source: an import must never be a legacy pack"
            );
            let reopened = ConsensusPack::open_static(target.path(), 0).expect("reopen import");
            for n in 1..=4 {
                reopened.get_consensus_output(n).await.expect("output reads back after reopen");
            }
            reopened.close().await;
        }

        let source = TempDir::with_prefix("test_import_v0_src").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&source, &committee, &chain, &previous_epoch, 4, 0).await;
        let target = TempDir::with_prefix("test_import_v0_dst").expect("temp dir");
        let Err(err) = ConsensusPack::stream_import(
            target.path(),
            epoch0_pack_stream(source.path()).await,
            0,
            &previous_epoch,
            4,
            Duration::from_secs(5),
        )
        .await
        else {
            panic!("a v0 source must be refused");
        };
        assert!(matches!(err, super::PackError::InvalidVersion(PACK_VERSION, 0)), "got {err:?}");
        assert!(
            !target.path().join("epoch-0").join(Inner::DATA_NAME).exists(),
            "nothing is written for a refused v0 source"
        );
    }

    /// The v1/v2 import streams each output's batches straight into the pack instead of buffering
    /// the whole output, so the (committee-scaled) per-output decode budget never applies to it:
    /// with that budget forced down to a single byte, a multi-batch epoch still imports.
    #[tokio::test]
    async fn test_stream_import_does_not_buffer_whole_outputs() {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        let v2_source = TempDir::with_prefix("test_import_stream_v2").expect("temp dir");
        build_test_pack_version(&v2_source, &committee, &chain, &previous_epoch, 3, PACK_VERSION)
            .await;
        {
            let original = ConsensusPack::open_static(v2_source.path(), 0).expect("open source");
            let output = original.get_consensus_output(1).await.expect("source output");
            assert!(
                output.batches().iter().any(|cb| !cb.batches.is_empty()),
                "fixture outputs must carry batches for this test to mean anything"
            );
            original.close().await;
        }

        super::TEST_OUTPUT_BUFFER_BUDGET.with(|c| c.set(Some(1)));
        let streamed = TempDir::with_prefix("test_import_stream_v2_dst").expect("temp dir");
        let streamed_res = ConsensusPack::stream_import(
            streamed.path(),
            epoch0_pack_stream(v2_source.path()).await,
            0,
            &previous_epoch,
            3,
            Duration::from_secs(5),
        )
        .await;
        // Reset before asserting so a failure doesn't leak the override into other tests on this
        // thread.
        super::TEST_OUTPUT_BUFFER_BUDGET.with(|c| c.set(None));

        let pack = streamed_res.expect("a streamed v2 import must not be bounded by the buffer");
        for n in 1..=3 {
            pack.get_consensus_output(n).await.expect("streamed output reads back");
        }
        pack.close().await;
    }

    /// At the import boundary, bytes the sender produced badly are told apart from a transport
    /// failure. A record that frames but fails its CRC is `UndecodableRecord` (the requester
    /// charges the peer), and so is a stream that ENDS cleanly in the middle of a record (or
    /// before its header): the peer's sync reader only reports a clean end after the peer's own
    /// `End` frame, so the short stream is the sender's. A transport failure mid-record (the
    /// reader errors, e.g. `ConnectionAborted`) is a `ReadError`, never charged.
    #[tokio::test]
    async fn test_stream_import_classifies_bad_bytes_vs_transport_failures() {
        /// A reader that fails the way a dropped connection does.
        struct Aborted;
        impl tokio::io::AsyncRead for Aborted {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionAborted.into()))
            }
        }

        let source = TempDir::with_prefix("test_import_fault_src").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&source, &committee, &chain, &previous_epoch, 3, PACK_VERSION)
            .await;
        let data_file = source.path().join("epoch-0").join(Inner::DATA_NAME);
        let mut logical = std::fs::read(&data_file).expect("read pack");
        logical.truncate(logical.len() - crate::archive::data_file::SENTINEL_LEN as usize);
        // Record start offsets: meta, then per output its header and its 4 batches.
        let mut starts = Vec::new();
        {
            let pack =
                Pack::<PackRecord>::open(&data_file, 0, true, PackCompression::ZStd, PACK_VERSION)
                    .expect("open pack");
            let mut iter = pack.raw_iter().expect("iter");
            loop {
                let pos = iter.logical_position();
                match iter.next() {
                    None => break,
                    Some(record) => {
                        record.expect("clean record");
                        starts.push(pos as usize);
                    }
                }
            }
        }
        // A batch record of output 2 (records: 0 meta, 1-5 output 1, 6-10 output 2).
        let batch = starts[8];
        let import = |bytes: Vec<u8>, transport_failure: bool| {
            use tokio::io::AsyncReadExt as _;
            let previous_epoch = previous_epoch.clone();
            async move {
                let target = TempDir::with_prefix("test_import_fault_dst").expect("temp dir");
                let stream: std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>> =
                    if transport_failure {
                        Box::pin(std::io::Cursor::new(bytes).chain(Aborted))
                    } else {
                        Box::pin(std::io::Cursor::new(bytes))
                    };
                ConsensusPack::stream_import(
                    target.path(),
                    stream,
                    0,
                    &previous_epoch,
                    3,
                    Duration::from_secs(5),
                )
                .await
                .expect_err("a damaged stream must not import")
            }
        };

        let mut corrupt = logical.clone();
        corrupt[batch + 8] ^= 0xFF;
        let err = import(corrupt, false).await;
        assert!(matches!(err, super::PackError::UndecodableRecord(_)), "CRC-bad record: {err:?}");

        let cut = logical[..batch + 6].to_vec();
        let err = import(cut.clone(), false).await;
        assert!(
            matches!(err, super::PackError::UndecodableRecord(_)),
            "stream ending mid-record: {err:?}"
        );
        let err = import(Vec::new(), false).await;
        assert!(
            matches!(err, super::PackError::UndecodableRecord(_)),
            "stream ending before its header: {err:?}"
        );
        let err = import(cut, true).await;
        assert!(
            matches!(err, super::PackError::ReadError(_)),
            "transport failure mid-record: {err:?}"
        );
    }

    /// PEER PATH: `stream_import` rejects an [`EpochMeta`] whose committee has a single authority
    /// as the sender's undecodable bytes, without panicking and before appending anything.
    ///
    /// No [`Committee`] constructor builds a one-member committee, so the hostile record is written
    /// through test-local shadow types that mirror [`PackRecord::EpochMeta`]'s bcs layout. Two
    /// checks pin the shadow before it is trusted: its encoding equals the real record's byte for
    /// byte, and a well-formed shadow imports far enough to be rejected by `verify_epoch_meta`.
    /// Only the committee's own decode validation can then reject the hostile one.
    #[tokio::test]
    async fn test_stream_import_rejects_single_authority_committee_meta() {
        use std::collections::BTreeMap;

        use serde::{Deserialize, Serialize};
        use tn_types::{Authority, BlockNumHash, BlsPublicKey, BootstrapServer};

        use crate::consensus_pack::{EpochMeta, PackError};

        /// Post-fork wire layout of a [`Committee`]: bcs writes no field names, so only the field
        /// types and their order decide the bytes.
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct WireCommittee {
            authorities: BTreeMap<BlsPublicKey, Authority>,
            epoch: Epoch,
            bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
            num_workers: NonZeroUsize,
        }

        /// Wire layout of an [`EpochMeta`], in its field order.
        #[derive(Debug, Serialize, Deserialize)]
        struct WireEpochMeta {
            epoch: Epoch,
            committee: WireCommittee,
            start_consensus_number: u64,
            genesis_exec_state: BlockNumHash,
            genesis_consensus: ConsensusNumHash,
        }

        /// Wire layout of [`PackRecord`] up to its first variant: bcs tags a variant by its index,
        /// so `EpochMeta` must stay first here as it is there.
        #[derive(Debug, Serialize, Deserialize)]
        enum WirePackRecord {
            EpochMeta(WireEpochMeta),
        }

        /// Write `record` as the only record of an epoch-0 pack, stream-import it into `target`
        /// and return the import's error.
        async fn import_meta(
            target: &Path,
            record: &WirePackRecord,
            previous_epoch: &EpochRecord,
        ) -> PackError {
            let source_dir = TempDir::with_prefix("test_cp_meta_single_src").expect("temp dir");
            let source = source_dir.path().join("peer_stream");
            {
                let mut pack: Pack<WirePackRecord> =
                    Pack::open(&source, 0, false, PackCompression::ZStd, PACK_VERSION)
                        .expect("open peer stream");
                pack.append(record).expect("append meta");
                pack.commit().expect("commit peer stream");
            }
            let stream = tokio::fs::File::open(&source).await.expect("open peer stream");
            ConsensusPack::stream_import(
                target,
                stream,
                0,
                previous_epoch,
                1,
                Duration::from_secs(5),
            )
            .await
            .expect_err("the meta must not import")
        }

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        // u32::MAX is past every multi-workers fork point, so the real committee encodes in the
        // post-fork layout the shadow mirrors under any feature set or fork override.
        let real = committee.advance_epoch_for_test(u32::MAX);
        let wire_committee = WireCommittee {
            authorities: real.authorities().into_iter().map(|a| (*a.protocol_key(), a)).collect(),
            epoch: real.epoch(),
            bootstrap_servers: real.bootstrap_servers(),
            num_workers: NonZeroUsize::new(real.number_of_workers()).expect("at least one worker"),
        };
        let wire_meta = |committee: WireCommittee| {
            WirePackRecord::EpochMeta(WireEpochMeta {
                epoch: 0,
                committee,
                start_consensus_number: 1,
                genesis_exec_state: previous_epoch.final_state,
                genesis_consensus: previous_epoch.final_consensus,
            })
        };

        // (1) layout: the shadow encodes byte for byte as the real record.
        let real_meta = PackRecord::EpochMeta(EpochMeta {
            epoch: 0,
            committee: real,
            start_consensus_number: 1,
            genesis_exec_state: previous_epoch.final_state,
            genesis_consensus: previous_epoch.final_consensus,
        });
        assert_eq!(
            tn_types::encode(&wire_meta(wire_committee.clone())),
            tn_types::encode(&real_meta),
            "the shadow types no longer match the PackRecord::EpochMeta wire layout"
        );

        // (2) control: the real decoder reads a well-formed shadow and gets as far as
        // `verify_epoch_meta`, which rejects only the committee's epoch.
        let target = TempDir::with_prefix("test_cp_meta_single_ctl").expect("temp dir");
        let err =
            import_meta(target.path(), &wire_meta(wire_committee.clone()), &previous_epoch).await;
        assert!(matches!(err, PackError::InvalidEpoch(0, _)), "got {err:?}");
        assert!(
            err.to_string().contains(&format!("committee is for epoch {}", u32::MAX)),
            "a different check rejected the control import: {err}"
        );

        // (3) hostile: the same meta with only the first authority. the sender's bytes do not
        // decode, so the import fails as an undecodable record (the requester charges the peer)
        // instead of panicking in the import task.
        let single = WireCommittee {
            authorities: wire_committee.authorities.into_iter().take(1).collect(),
            ..wire_committee
        };
        let target = TempDir::with_prefix("test_cp_meta_single_out").expect("temp dir");
        let err = import_meta(target.path(), &wire_meta(single), &previous_epoch).await;
        let PackError::UndecodableRecord(msg) = &err else {
            panic!("a single-authority meta must be an undecodable record, got {err:?}")
        };
        assert!(
            msg.contains("at least 2 authorities"),
            "a different check rejected the hostile meta: {msg}"
        );

        // rejected before the append: nothing follows the data file's header, if the import got as
        // far as creating the file, so no reader can pick the hostile committee up.
        let data_file = target.path().join("epoch-0").join(Inner::DATA_NAME);
        let appended = data_file.exists()
            && Pack::<PackRecord>::open(&data_file, 0, true, PackCompression::ZStd, PACK_VERSION)
                .expect("reopen the import's data file")
                .record_present_at(DATA_HEADER_BYTES as u64);
        assert!(!appended, "the rejected meta was appended anyway");
    }

    /// The import stops reading at the requested final: outputs a peer streams past it are never
    /// read, so they can neither be imported nor fail an otherwise-valid download.
    #[tokio::test]
    async fn test_stream_import_stops_at_final() {
        let source = TempDir::with_prefix("test_import_final_src").expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let previous_epoch = test_previous_epoch(&committee);
        build_test_pack_version(&source, &committee, &chain, &previous_epoch, 5, PACK_VERSION)
            .await;

        let target = TempDir::with_prefix("test_import_final_dst").expect("temp dir");
        let pack = ConsensusPack::stream_import(
            target.path(),
            epoch0_pack_stream(source.path()).await,
            0,
            &previous_epoch,
            3,
            Duration::from_secs(5),
        )
        .await
        .expect("an import must stop cleanly at its final");
        let latest = pack.latest_consensus_header().await.expect("latest").expect("has outputs");
        assert_eq!(latest.number, 3, "the import must end exactly at the requested final");
        assert!(
            !pack.contains_consensus_header_number(4).await.expect("query"),
            "an output past the final must not be imported"
        );
        pack.close().await;
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
    /// multi-workers fork (issue #554), and the same epoch `tn_types`' `LEGACY_FIXTURE_EPOCH`
    /// pins — so the frozen committee vector there and the frozen pack file here describe one wire
    /// moment from opposite ends of the stack.
    ///
    /// One below the floor rather than the floor itself, because the floor is itself a legal fork
    /// epoch: the gate is `>=`, so a fork epoch of 407 would make 407 post-fork and fail the
    /// anti-vacuity assert in `test_golden_legacy_pack_regenerates`. The adiri fork epoch is 570
    /// (floored at 407), so epoch 406 here is pre-fork, and it stays pre-fork under any fork epoch
    /// the floor allows: the embedded [`Committee`] encodes in the legacy single-worker layout. It
    /// is still at or above `SEED_SIGNATURE_FORK_EPOCH` (383), so the nested headers carry
    /// `seed_signature`: exactly the shape of an epoch pack sitting on an adiri node's disk today.
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

    /// Read a cleanly-closed pack's data file and return its **logical** (served) bytes — the whole
    /// file minus the trailing 8-byte clean-close sentinel a clean close appends. Serving/export is
    /// bounded to exactly this range, so this is the byte-for-byte view a pre-fork peer receives;
    /// the on-disk sentinel is never served. The frozen `GOLDEN_LEGACY_PACK_HEX` fixture is the
    /// wire format, so byte-identity assertions compare against this logical view.
    #[cfg(feature = "adiri")]
    fn read_logical_data_file(dir: &std::path::Path) -> Vec<u8> {
        let path = dir.join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME);
        let mut whole = std::fs::read(&path).expect("read data file");
        let logical = whole
            .len()
            .checked_sub(crate::archive::data_file::SENTINEL_LEN as usize)
            .expect("a cleanly-closed pack must carry the clean-close sentinel");
        whole.truncate(logical);
        whole
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
    /// Two is the minimum — `Committee::new` (builder) and `CommitteeInner::validate` (decode)
    /// both refuse a committee of one — and the point of the fixture is the wire layout, not the
    /// quorum math, so it stays at the minimum to keep the frozen vector small. One worker per
    /// server is the only shape the legacy layout can express at all.
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
        let mut outputs = Vec::new();
        for (number, tag) in (previous.final_consensus.number + 1..).zip([1_u8, 2]) {
            let output = legacy_pack_output(committee, number, parent, tag);
            parent = output.digest();
            outputs.push(output);
        }
        outputs
    }

    /// Write the fixture pack through the normal write path (`save_consensus_output`) into `dir`,
    /// pinned to the pre-fork data-file version, and return the resulting `data` file bytes.
    ///
    /// On the `adiri` lane at [`LEGACY_PACK_EPOCH`] the gated encoder emits the legacy committee
    /// layout, which `tn_types`' differentials prove is byte-identical to the pre-#554 derive. A
    /// pack this writes at that epoch therefore IS a pre-fork pack, byte for byte — which is what
    /// makes freezing its output a fixture of history rather than of this build. The version is
    /// pinned to v1 explicitly: `open_append` now stamps the current `PACK_VERSION` (v2, which adds
    /// the clean-close sentinel) into fresh files, so regenerating through the version-pinning door
    /// keeps the fixture a faithful pre-sentinel artifact.
    #[cfg(feature = "adiri")]
    async fn write_legacy_pack(dir: &std::path::Path) -> Vec<u8> {
        let committee = legacy_pack_committee();
        let previous_epoch = legacy_pack_previous_epoch(&committee);
        let pack =
            ConsensusPack::open_append_version(dir, previous_epoch.clone(), committee.clone(), 1)
                .expect("open fixture pack for append");
        for output in legacy_pack_outputs(&committee, &previous_epoch) {
            pack.save_consensus_output(output).await.expect("save fixture output");
        }
        pack.persist().await.expect("persist fixture pack");
        drop(pack);
        // Return the logical (served) bytes — the frozen fixture is the wire format, without the
        // on-disk clean-close sentinel.
        read_logical_data_file(dir)
    }

    /// Materialize a complete pack directory (data file plus sidecar indexes) in `dir` FROM the
    /// frozen bytes, by feeding them to `stream_import` the way a peer stream arrives.
    ///
    /// `stream_import` is the only path that builds a pack's indexes from a record stream, so it is
    /// how the doors that need sidecars (`open_static`, and reading outputs back through
    /// `open_append_exists` / `open_append`) get an on-disk pack whose records are provably the
    /// frozen records — asserted here, so every caller inherits the guarantee. The import writes
    /// the current (v2) format, so only the data header differs from the frozen v1 bytes.
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
        assert_frozen_records_as_v2(dir, "stream import");
    }

    /// Assert the pack under `dir` holds the frozen fixture's records byte for byte (so the legacy
    /// committee layout inside the meta, and every header and batch, are exactly as frozen) under a
    /// current-format (v2) data header. The import and the writable doors store v2 (v2 is the only
    /// writable format); the records are carried over unchanged.
    #[cfg(feature = "adiri")]
    fn assert_frozen_records_as_v2(dir: &std::path::Path, what: &str) {
        use crate::archive::pack::DATA_HEADER_BYTES;

        let on_disk = read_logical_data_file(dir);
        let frozen = golden_legacy_pack_bytes();
        assert_eq!(
            tn_types::hex::encode(&on_disk[DATA_HEADER_BYTES..]),
            tn_types::hex::encode(&frozen[DATA_HEADER_BYTES..]),
            "{what}: the frozen pre-fork records were rewritten"
        );
        let data_path = dir.join(format!("epoch-{LEGACY_PACK_EPOCH}")).join(Inner::DATA_NAME);
        let version = Pack::<PackRecord>::open(
            &data_path,
            LEGACY_PACK_EPOCH as u64,
            true,
            PackCompression::ZStd,
            PACK_VERSION,
        )
        .expect("open data read-only")
        .version();
        assert_eq!(version, PACK_VERSION, "{what}: the pack must be stored in the current format");
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
    /// frozen layout, so a stray `TN_MULTI_WORKERS_FORK_EPOCH` in the environment (or a fork epoch
    /// moved below its 407 floor) fails here with a diagnosis instead of downstream as an
    /// unexplained byte diff.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_regenerates() {
        use tn_types::{encode, forks};

        assert!(
            !forks::multi_workers_fork_active(LEGACY_PACK_EPOCH),
            "epoch {LEGACY_PACK_EPOCH} must be PRE-fork for the frozen pack to be a legacy-layout \
             pack; is TN_MULTI_WORKERS_FORK_EPOCH set in the environment, or has the fork epoch \
             been moved below its 407 floor?"
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
    /// outputs must also read back. v2 is the only writable format, so the writable door migrates
    /// the frozen v1 pack to v2; the migration must carry every record over byte for byte.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_opens_append_exists() {
        let bare = TempDir::with_prefix("golden_legacy_warm_bare").expect("temp dir");
        write_golden_legacy_data_file(bare.path());
        {
            let pack = ConsensusPack::open_append_exists(bare.path(), LEGACY_PACK_EPOCH)
                .expect("warm restart against a bare frozen pre-fork data file");
            // The frozen fixture is a v1 (pre-sentinel) pack; the writable door migrates it to the
            // current format before appending anything.
            assert_eq!(pack.version, PACK_VERSION, "a warm restart must migrate the v1 pack to v2");
            assert!(!pack.is_static(), "a warm-restart handle is writable");
            assert_legacy_pack_committee(&pack);
            pack.persist().await.expect("persist");
        }
        assert_frozen_records_as_v2(bare.path(), "warm restart");

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
    /// The load-bearing assertion is record identity: importing and then serving a pre-fork pack
    /// must leave the meta record (with its legacy committee layout) and every output exactly as it
    /// arrived. The import stores them under a v2 data header, the only writable format.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_golden_legacy_pack_stream_imports() {
        let dir = TempDir::with_prefix("golden_legacy_import").expect("temp dir");
        // asserts the imported data file holds the frozen records under a v2 header
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

        assert_frozen_records_as_v2(dir.path(), "import then serve");
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
        assert_frozen_records_as_v2(dir.path(), "reopen for append");

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

        // the warm-restart door: the pack is a pre-v2 format, so this door first tries to migrate
        // it up to v2 — but the legacy-layout meta cannot decode on a post-fork build, so
        // the migration reports the pack corrupt (re-sync) and the open fails loudly rather
        // than misparsing the committee. (Before the pre-v2 pack was opened directly and
        // failed with `EpochLoad`; either way the meta-decode failure is surfaced, never
        // silently accepted.)
        let warm = ConsensusPack::open_append_exists(dir.path(), LEGACY_PACK_EPOCH);
        assert!(
            matches!(
                warm,
                Err(super::PackError::CorruptPack(_)) | Err(super::PackError::EpochLoad(_))
            ),
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
