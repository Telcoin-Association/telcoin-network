//! Where the resolver reads a batch the cache does not hold: the consensus packs.
//!
//! The packs are reached through the chain's `LiveBatches` (`ConsensusChain::live_batches`), which
//! holds the chain weakly: the resolver lives as long as the env and must never keep the chain
//! from closing.
//! - The live epoch is read through a clone of the live pack, dropped before the read returns.
//! - A closed epoch is read through a [`PackReader`] the resolver owns (`LiveBatches::open_past`,
//!   which tells an epoch handoff's seal in progress from a damaged pack). At most [`READERS`] of
//!   them are kept open, least recently used first out, and all are dropped when
//!   `ConsensusChain::install_generation` moves (an install renamed new files over an epoch, or an
//!   epoch handoff swapped the live pack).
//!
//! At most [`DECODE_PERMITS`] pack reads run at once, so a burst of history reads never occupies
//! more blocking threads than that, nor piles onto the live pack's read lock.

use std::{
    collections::VecDeque,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::{Condvar, Mutex};
use tn_storage::{
    consensus::{ConsensusChain, LiveBatches},
    consensus_pack::{BatchReadError, PackReader, DATA_NAME},
};
use tn_types::{Batch, Epoch, B256};

/// How many closed epochs' readers are kept open (each holds about 4 MiB of bloom filters and six
/// descriptors).
pub(crate) const READERS: usize = 4;

/// How many pack reads run at once.
pub(crate) const DECODE_PERMITS: usize = 4;

/// A batch read from one closed epoch's pack.
pub trait EpochBatches: Send + Sync {
    /// The batch stored under `digest`: `Ok(None)` only when the pack does not hold it.
    fn batch(&self, digest: B256) -> Result<Option<Batch>, BatchReadError>;
}

impl EpochBatches for PackReader {
    fn batch(&self, digest: B256) -> Result<Option<Batch>, BatchReadError> {
        PackReader::batch(self, digest)
    }
}

/// Why a closed epoch's pack did not open.
#[derive(Debug)]
pub enum PackOpenError {
    /// The epoch has no pack on disk.
    Missing,
    /// The epoch's pack is being replaced (an install's rename window): open again shortly.
    Installing,
    /// The epoch's pack is not sealed: the live epoch, or the epoch a handoff is sealing.
    Unsealed,
    /// Any other failure.
    Failed(BatchReadError),
}

/// The consensus packs, as the resolver reads them.
pub trait PackBatches: Send + Sync + fmt::Debug {
    /// A counter that moves whenever an epoch's files may have been replaced: every reader opened
    /// before it moved is dropped.
    fn install_generation(&self) -> u64;

    /// The batch under `digest` in `epoch`'s pack if `epoch` is the live one, else `None`.
    fn live_batch(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Option<Result<Option<Batch>, BatchReadError>>;

    /// A reader of closed epoch `epoch`'s pack.
    fn open(&self, epoch: Epoch) -> Result<Arc<dyn EpochBatches>, PackOpenError>;
}

/// The node's consensus packs: the live one through its [`ConsensusChain`] (held weakly, see
/// [`LiveBatches`]), the closed ones from `epochs_dir` (the directory holding the `epoch-{N}`
/// directories).
#[derive(Debug)]
pub struct ChainPacks {
    live: LiveBatches,
    epochs_dir: PathBuf,
}

impl ChainPacks {
    /// The packs of `chain`, whose epochs are under `epochs_dir`.
    pub fn new(chain: &ConsensusChain, epochs_dir: PathBuf) -> Self {
        Self { live: chain.live_batches(), epochs_dir }
    }

    /// Whether `epoch`'s pack, which an open just found missing, is being replaced: an install
    /// moves the old `epoch-{N}` directory to `epoch-{N}.replaced` before it renames the new one
    /// in, so the epoch is briefly missing while that exists; a data log found now was renamed in
    /// after the open looked.
    pub(crate) fn installing(epochs_dir: &Path, epoch: Epoch) -> bool {
        epochs_dir.join(format!("epoch-{epoch}.replaced")).exists()
            || epochs_dir.join(format!("epoch-{epoch}")).join(DATA_NAME).exists()
    }
}

impl PackBatches for ChainPacks {
    fn install_generation(&self) -> u64 {
        self.live.install_generation()
    }

    fn live_batch(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Option<Result<Option<Batch>, BatchReadError>> {
        self.live.live_batch(epoch, digest)
    }

    fn open(&self, epoch: Epoch) -> Result<Arc<dyn EpochBatches>, PackOpenError> {
        match self.live.open_past(&self.epochs_dir, epoch) {
            Ok(reader) => Ok(Arc::new(reader)),
            Err(BatchReadError::Unsealed) => Err(PackOpenError::Unsealed),
            Err(BatchReadError::Open(e)) if e.is_missing_static_files() => {
                if Self::installing(&self.epochs_dir, epoch) {
                    Err(PackOpenError::Installing)
                } else {
                    Err(PackOpenError::Missing)
                }
            }
            Err(e) => Err(PackOpenError::Failed(e)),
        }
    }
}

/// A counting semaphore whose acquire blocks the calling thread.
#[derive(Debug)]
struct Permits {
    free: Mutex<usize>,
    released: Condvar,
}

/// One held permit, given back on drop.
pub(crate) struct Permit<'a>(&'a Permits);

impl Permits {
    fn new(permits: usize) -> Self {
        Self { free: Mutex::new(permits), released: Condvar::new() }
    }

    fn acquire(&self) -> Permit<'_> {
        let mut free = self.free.lock();
        while *free == 0 {
            self.released.wait(&mut free);
        }
        *free -= 1;
        Permit(self)
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        *self.0.free.lock() += 1;
        self.0.released.notify_one();
    }
}

/// The open readers and the install generation they were opened under.
#[derive(Default)]
struct Readers {
    generation: u64,
    /// Most recently used first.
    open: VecDeque<(Epoch, Arc<dyn EpochBatches>)>,
}

/// The packs the resolver reads, with its own readers of closed epochs ([module docs](self)).
pub struct PackSource {
    packs: Box<dyn PackBatches>,
    readers: Mutex<Readers>,
    permits: Permits,
}

impl fmt::Debug for PackSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackSource").field("packs", &self.packs).finish_non_exhaustive()
    }
}

impl PackSource {
    /// A source reading `packs`.
    pub fn new(packs: impl PackBatches + 'static) -> Self {
        Self {
            packs: Box::new(packs),
            readers: Mutex::new(Readers::default()),
            permits: Permits::new(DECODE_PERMITS),
        }
    }

    /// Hold one of the pack-read permits.
    pub(crate) fn permit(&self) -> Permit<'_> {
        self.permits.acquire()
    }

    /// The live epoch's read (see [`PackBatches::live_batch`]).
    pub(crate) fn live_batch(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Option<Result<Option<Batch>, BatchReadError>> {
        self.packs.live_batch(epoch, digest)
    }

    /// A reader of closed epoch `epoch`, and whether it was opened by this call. Every reader is
    /// dropped first when the install generation moved since they were opened.
    pub(crate) fn reader(
        &self,
        epoch: Epoch,
    ) -> Result<(Arc<dyn EpochBatches>, bool), PackOpenError> {
        let generation = self.packs.install_generation();
        {
            let mut readers = self.readers.lock();
            if readers.generation != generation {
                readers.open.clear();
                readers.generation = generation;
            }
            if let Some(at) = readers.open.iter().position(|(e, _)| *e == epoch) {
                let entry = readers.open.remove(at).expect("found above");
                let reader = entry.1.clone();
                readers.open.push_front(entry);
                return Ok((reader, false));
            }
        }
        // open outside the lock: an open maps six files and reads the blooms
        let reader = self.packs.open(epoch)?;
        crate::metrics::record_tx_body_pack_open();
        let mut readers = self.readers.lock();
        // an install after the open began may have replaced what it opened: keep it for this read
        // only
        if readers.generation == generation {
            readers.open.retain(|(e, _)| *e != epoch);
            readers.open.push_front((epoch, reader.clone()));
            readers.open.truncate(READERS);
        }
        Ok((reader, true))
    }

    /// Drop the reader of `epoch`, so the next read opens the epoch again.
    pub(crate) fn forget(&self, epoch: Epoch) {
        self.readers.lock().open.retain(|(e, _)| *e != epoch);
    }

    /// The epochs whose readers are open, most recently used first.
    #[cfg(test)]
    pub(crate) fn open_epochs(&self) -> Vec<Epoch> {
        self.readers.lock().open.iter().map(|(e, _)| *e).collect()
    }
}
