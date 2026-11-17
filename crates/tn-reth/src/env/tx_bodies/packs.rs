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
//! At most [`DECODE_PERMITS`] pack reads run at once, and at most [`MAX_PERMIT_WAITERS`] threads
//! wait for one: a read that finds that many waiting fails at once, and one still waiting at its
//! resolve's deadline ([`PACK_WAIT`] after the resolve began) fails then, both as
//! [`PermitTimeout`](super::TxBodyErrorKind::PermitTimeout). So a burst of history reads never
//! has more than 128 blocking threads reading packs or waiting for a permit, nor piles onto the
//! live pack's read lock. A read that waits for a pack being sealed or replaced sleeps between its
//! tries without a permit, until the same deadline.

use std::{
    collections::VecDeque,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};
use tn_storage::{
    consensus::{ConsensusChain, LiveBatches},
    consensus_pack::{BatchReadError, PackReader, DATA_NAME},
};
use tn_types::{Batch, Epoch, B256};

use super::PACK_WAIT;

/// How many closed epochs' readers are kept open (each holds about 4 MiB of bloom filters and six
/// descriptors).
pub(crate) const READERS: usize = 4;

/// How many pack reads run at once.
pub(crate) const DECODE_PERMITS: usize = 4;

/// How many threads may wait for a pack-read permit: a read that finds this many waiting fails at
/// once. With the [`DECODE_PERMITS`] holders that is 128 threads node-wide, half of the 256 threads
/// each RPC server's blocking pool keeps beyond its server's blocking-IO permits
/// (`rpc_blocking::THREADS_BEYOND_PERMITS`). A read of transactions stored by position holds a
/// pool thread but no blocking-IO permit, so this keeps such reads, while they read a pack or wait
/// for a permit, from taking the threads the permit holders' work waits for.
pub(crate) const MAX_PERMIT_WAITERS: usize = 124;

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

/// A counting semaphore whose acquire blocks the calling thread, up to a deadline, and refuses at
/// once while [`MAX_PERMIT_WAITERS`] threads wait.
#[derive(Debug)]
struct Permits {
    counts: Mutex<PermitCounts>,
    released: Condvar,
}

/// The free permits, and the threads waiting for one.
#[derive(Debug)]
struct PermitCounts {
    free: usize,
    waiting: usize,
}

/// One held permit, given back on drop.
pub(crate) struct Permit<'a>(&'a Permits);

impl Permits {
    fn new(permits: usize) -> Self {
        Self {
            counts: Mutex::new(PermitCounts { free: permits, waiting: 0 }),
            released: Condvar::new(),
        }
    }

    /// A free permit at once, else the first one given back before `deadline`. `None` when none
    /// is given back by then, or at once when [`MAX_PERMIT_WAITERS`] threads already wait.
    fn acquire(&self, deadline: Instant) -> Option<Permit<'_>> {
        let mut counts = self.counts.lock();
        if counts.free == 0 {
            if counts.waiting >= MAX_PERMIT_WAITERS {
                return None;
            }
            counts.waiting += 1;
            while counts.free == 0 {
                if self.released.wait_until(&mut counts, deadline).timed_out() {
                    break;
                }
            }
            counts.waiting -= 1;
            // a permit given back as the wait timed out is still taken
            if counts.free == 0 {
                return None;
            }
        }
        counts.free -= 1;
        Some(Permit(self))
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.0.counts.lock().free += 1;
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
    /// How long one resolve may wait in all: [`PACK_WAIT`], shorter or longer only in tests.
    wait: Duration,
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
            wait: PACK_WAIT,
        }
    }

    /// This source, its resolves waiting up to `wait` in all instead of [`PACK_WAIT`].
    #[cfg(test)]
    pub(crate) fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// How long one resolve may wait in all, from when it begins: for pack-read permits, and for a
    /// closed epoch's pack being sealed or replaced.
    pub(crate) fn wait(&self) -> Duration {
        self.wait
    }

    /// Hold one of the pack-read permits: `None` when none is given back before `deadline`, or at
    /// once when [`MAX_PERMIT_WAITERS`] threads already wait for one.
    pub(crate) fn permit(&self, deadline: Instant) -> Option<Permit<'_>> {
        self.permits.acquire(deadline)
    }

    /// How many pack-read permits are free, and how many threads wait for one.
    #[cfg(test)]
    pub(crate) fn permit_counts(&self) -> (usize, usize) {
        let counts = self.permits.counts.lock();
        (counts.free, counts.waiting)
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
