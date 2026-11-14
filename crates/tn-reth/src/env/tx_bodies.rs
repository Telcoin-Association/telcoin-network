//! The transactions a pointer datadir stores by position (layout marker version 2,
//! `env::layout`), read back from the consensus batches they were executed from.
//!
//! Such a datadir's `Transactions` rows hold each transaction's position in its block's batch.
//! The vendored provider resolves those rows through the [`TxBodyResolver`] installed on the
//! static files, which is a [`TxBodySource`] on every env (a datadir that stores transactions
//! inline never asks it). For block `number` with `header` and the positions asked it:
//! 1. takes the batch digest from `header.ommers_hash` (zero is an error: the block names no batch)
//!    and the epoch from the nonce's high 32 bits, the epoch whose pack holds the batch;
//! 2. finds the batch in the decoded-batch cache (`cache`); in a test build, in the
//!    [`MemoryBatches`] that stands in for packs; else in the packs (`packs`): the live epoch's,
//!    else a closed epoch's, whose read checks the batch digest again. A batch read from a pack
//!    goes into the cache's cold part;
//! 3. decodes the transaction at each position (EIP-2718), each position checked against the
//!    batch's length.
//!
//! Every failure is a [`TxBodyError`] naming the block, the epoch and the digest, returned as a
//! provider error: never a missing transaction, never another one.
//!
//! The cache's recent part is filled when a persist stores a batch's transactions by position
//! ([`TxBodyCacheFill`], the env's [`PersistedBatchSink`]), on a blocking task: a read before the
//! fill lands goes to the live pack.
//!
//! The packs are attached at startup by [`RethEnv::attach_tx_packs`], which first refuses a
//! datadir whose rows can point into a pack that is not on disk ([`TxPackCoverageError`]).

mod cache;
mod coverage;
mod packs;
#[cfg(test)]
mod tests;
// pointer datadirs are storage v2, which runs on the tn-lmdb build only
#[cfg(all(test, not(feature = "mdbx")))]
mod pack_tests;

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use reth_provider::{providers::TxBodyResolver, ProviderError, ProviderResult};
use tn_storage::consensus_pack::BatchReadError;
use tn_types::{
    Batch, BlockNumber, Decodable2718 as _, Epoch, ExecHeader, TransactionSigned, B256,
};

pub use cache::TX_BODY_CACHE_BYTES;
pub(crate) use cache::{BatchCache, CachePart, CachedBatch};
pub use coverage::TxPackCoverageError;
pub use packs::{ChainPacks, EpochBatches, PackBatches, PackOpenError, PackSource};

use crate::{
    env::{PersistedBatchSink, PersistedBatches},
    RethEnv, TNPrimitives,
};

/// How long a read waits for a closed epoch's pack that is not sealed yet (the epoch an epoch
/// handoff is sealing) or is being replaced (an install's rename window).
const PACK_WAIT: Duration = Duration::from_secs(1);

/// How often such a read tries the pack again.
const PACK_RETRY: Duration = Duration::from_millis(10);

/// A pointer row that could not be resolved: block `block`'s transactions are stored by position
/// in batch `digest`, which epoch `epoch`'s consensus pack holds.
#[derive(Debug, thiserror::Error)]
#[error(
    "the transactions of block {block} are stored by position in batch {digest} of epoch {epoch}: \
     {kind}"
)]
pub struct TxBodyError {
    /// The block.
    pub block: BlockNumber,
    /// The epoch its header's nonce names.
    pub epoch: Epoch,
    /// The batch digest its header's `ommers_hash` names.
    pub digest: B256,
    /// What failed.
    pub kind: TxBodyErrorKind,
}

/// What failed in a [`TxBodyError`].
#[derive(Debug, thiserror::Error)]
pub enum TxBodyErrorKind {
    /// The header's `ommers_hash` is zero: it names no batch.
    #[error("the block names no batch")]
    NoBatchDigest,
    /// No consensus packs are attached to this env (a tool opened a datadir that stores
    /// transactions by position).
    #[error("no consensus packs are attached to read it from")]
    PacksNotAttached,
    /// The epoch's pack is not on disk.
    #[error("the epoch's consensus pack is missing")]
    PackMissing,
    /// The epoch's pack does not hold the batch.
    #[error("the epoch's consensus pack does not hold the batch")]
    BatchMissing,
    /// The pack read failed, a batch digest check included.
    #[error("the batch could not be read: {0}")]
    Read(Box<BatchReadError>),
    /// A position is past the batch's last transaction.
    #[error("position {pos} is past the batch's {len} transactions")]
    PositionOutOfRange {
        /// The position asked.
        pos: u32,
        /// The batch's transaction count.
        len: usize,
    },
    /// The transaction at a position does not decode.
    #[error("the transaction at position {pos} does not decode: {error}")]
    Decode {
        /// The position.
        pos: u32,
        /// The decode error.
        error: String,
    },
    /// The batch's transactions exceed what the cache's offsets address (4 GiB).
    #[error("the batch's transactions exceed 4 GiB")]
    BatchTooLarge,
    /// The epoch's pack stayed unsealed, or its replacement unfinished, for [`PACK_WAIT`]: an
    /// epoch handoff that did not finish, a past epoch left unsealed, or a pack missing files
    /// beside its data log (`db validate`).
    #[error(
        "the epoch's consensus pack stayed unsealed, mid-install or without all its files for \
         {0:?}"
    )]
    HandoffTimeout(Duration),
}

impl TxBodyErrorKind {
    /// Every `reason` label of `tn_reth.tx_body_resolve_failures_total`.
    pub(crate) const LABELS: [&'static str; 9] = [
        "no_batch_digest",
        "packs_not_attached",
        "pack_missing",
        "batch_missing",
        "read",
        "position_out_of_range",
        "decode",
        "batch_too_large",
        "handoff_timeout",
    ];

    /// This failure's `reason` label.
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::NoBatchDigest => Self::LABELS[0],
            Self::PacksNotAttached => Self::LABELS[1],
            Self::PackMissing => Self::LABELS[2],
            Self::BatchMissing => Self::LABELS[3],
            Self::Read(_) => Self::LABELS[4],
            Self::PositionOutOfRange { .. } => Self::LABELS[5],
            Self::Decode { .. } => Self::LABELS[6],
            Self::BatchTooLarge => Self::LABELS[7],
            Self::HandoffTimeout(_) => Self::LABELS[8],
        }
    }
}

/// Where a resolve found its batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BatchSource {
    /// The cache's recent or cold part.
    Cache(CachePart),
    /// The live epoch's pack.
    LivePack,
    /// A closed epoch's pack.
    ClosedPack,
    /// The test builds' [`MemoryBatches`].
    #[cfg(any(test, feature = "test-utils"))]
    Memory,
}

impl BatchSource {
    /// Every `source` label of `tn_reth.tx_body_resolves_total`.
    pub(crate) const LABELS: [&'static str; 5] =
        ["recent", "cold", "live_pack", "closed_pack", "memory"];

    /// This source's `source` label.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Cache(part) => part.label(),
            Self::LivePack => Self::LABELS[2],
            Self::ClosedPack => Self::LABELS[3],
            #[cfg(any(test, feature = "test-utils"))]
            Self::Memory => Self::LABELS[4],
        }
    }
}

/// Test builds' stand-in for the consensus packs: every batch remembered, by digest, unbounded.
///
/// Temp chains that store transactions by position share [`MemoryBatches::global`], so a datadir
/// reopened in the same process still resolves. A batch is remembered when a persist stores its
/// transactions by position ([`TxBodyCacheFill`]), before the persist's caller goes on, unless
/// the instance is [`MemoryBatches::filled_by_caller`].
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default)]
pub struct MemoryBatches {
    batches: parking_lot::Mutex<std::collections::HashMap<B256, Arc<CachedBatch>>>,
    /// Whether only [`MemoryBatches::insert`] adds batches: persists do not.
    caller_fills: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryBatches {
    /// The process-wide instance temp chains use.
    pub fn global() -> Arc<Self> {
        static GLOBAL: std::sync::LazyLock<Arc<MemoryBatches>> =
            std::sync::LazyLock::new(Default::default);
        GLOBAL.clone()
    }

    /// An instance its owner fills with [`Self::insert`], each batch before the persist that
    /// stores it by position: persists add nothing to it, so they do only what a node's persist
    /// does (the cache fill on a blocking task). The execution bench holds its outputs' batches
    /// here, so its timed outputs do not pay the copy a remembering persist makes.
    pub fn filled_by_caller() -> Self {
        Self { caller_fills: true, ..Self::default() }
    }

    /// Remember `batch` under `digest`.
    pub fn insert(&self, digest: B256, batch: &Batch) {
        if let Some(cached) = CachedBatch::of(batch) {
            self.batches.lock().insert(digest, Arc::new(cached));
        }
    }

    /// How many batches it holds.
    pub fn len(&self) -> usize {
        self.batches.lock().len()
    }

    /// Whether it holds no batch.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The batch remembered under `digest`.
    fn get(&self, digest: &B256) -> Option<Arc<CachedBatch>> {
        self.batches.lock().get(digest).cloned()
    }
}

/// The resolver of a datadir's pointer rows ([module docs](self)), installed on its static files.
#[derive(Debug)]
pub struct TxBodySource {
    /// The decoded-batch cache.
    cache: BatchCache,
    /// The consensus packs, attached once the node has opened them.
    packs: OnceLock<PackSource>,
    /// Test builds: the stand-in for packs, if this env uses one.
    #[cfg(any(test, feature = "test-utils"))]
    memory: Option<Arc<MemoryBatches>>,
}

impl TxBodySource {
    /// A source with a decoded-batch cache of `cache_bytes` (0: none) and no packs.
    pub(crate) fn new(cache_bytes: usize) -> Self {
        Self {
            cache: BatchCache::new(cache_bytes),
            packs: OnceLock::new(),
            #[cfg(any(test, feature = "test-utils"))]
            memory: None,
        }
    }

    /// This source, reading `memory` before any pack.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn with_memory_batches(mut self, memory: Arc<MemoryBatches>) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Whether this source reads a [`MemoryBatches`].
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn has_memory_batches(&self) -> bool {
        self.memory.is_some()
    }

    /// Attach the packs this source reads batches the cache does not hold from. Once only.
    pub(crate) fn attach_packs(&self, packs: PackSource) -> eyre::Result<()> {
        self.packs
            .set(packs)
            .map_err(|_| eyre::eyre!("consensus packs are already attached to the tx-body source"))
    }

    /// Copy `batches`, which a persist just stored by position, into the cache's recent part.
    /// Copies each batch's bytes (a megabyte or so): call it off the execution thread.
    pub fn fill_recent<'a>(&self, batches: impl IntoIterator<Item = (B256, &'a Batch)>) {
        for (digest, batch) in batches {
            if let Some(cached) = CachedBatch::of(batch) {
                self.cache.insert_recent(digest, Arc::new(cached));
            }
        }
    }

    /// Test builds: remember `batches` in this source's [`MemoryBatches`], if it has one that
    /// persists fill (not [`MemoryBatches::filled_by_caller`]).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn remember<'a>(&self, batches: impl IntoIterator<Item = (B256, &'a Batch)>) {
        if let Some(memory) = self.memory.as_ref().filter(|memory| !memory.caller_fills) {
            for (digest, batch) in batches {
                memory.insert(digest, batch);
            }
        }
    }

    /// The transactions at `positions` of the batch block `number`'s `header` names, in order.
    pub(crate) fn transactions(
        &self,
        number: BlockNumber,
        header: &ExecHeader,
        positions: &[u32],
    ) -> Result<Vec<TransactionSigned>, TxBodyError> {
        if positions.is_empty() {
            return Ok(Vec::new());
        }
        let digest = header.ommers_hash;
        let epoch = RethEnv::extract_epoch_from_header(header);
        let resolved = self
            .batch(epoch, digest)
            .and_then(|(batch, source)| decode(&batch, positions).map(|txs| (txs, source)));
        match resolved {
            Ok((txs, source)) => {
                crate::metrics::record_tx_body_resolve(source.label());
                Ok(txs)
            }
            Err(kind) => {
                crate::metrics::record_tx_body_resolve_failure(kind.label());
                Err(TxBodyError { block: number, epoch, digest, kind })
            }
        }
    }

    /// The batch `digest` of `epoch`, and where it was found.
    fn batch(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Result<(Arc<CachedBatch>, BatchSource), TxBodyErrorKind> {
        if digest == B256::ZERO {
            return Err(TxBodyErrorKind::NoBatchDigest);
        }
        if let Some((batch, part)) = self.cache.get(&digest) {
            return Ok((batch, BatchSource::Cache(part)));
        }
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(memory) = &self.memory {
            if let Some(batch) = memory.get(&digest) {
                return Ok((batch, BatchSource::Memory));
            }
            if self.packs.get().is_none() {
                return Err(TxBodyErrorKind::BatchMissing);
            }
        }
        let packs = self.packs.get().ok_or(TxBodyErrorKind::PacksNotAttached)?;
        let started = Instant::now();
        let (batch, source) = read_packs(packs, epoch, digest)?;
        let batch = Arc::new(CachedBatch::of(&batch).ok_or(TxBodyErrorKind::BatchTooLarge)?);
        crate::metrics::record_tx_body_resolve_seconds(started.elapsed());
        self.cache.insert_cold(digest, batch.clone());
        Ok((batch, source))
    }

    /// The cache, for tests.
    #[cfg(test)]
    pub(crate) fn cache(&self) -> &BatchCache {
        &self.cache
    }

    /// The attached packs, for tests.
    #[cfg(test)]
    pub(crate) fn packs(&self) -> Option<&PackSource> {
        self.packs.get()
    }
}

/// Batch `digest` of `epoch` from the packs: the live pack's if `epoch` is live, else the closed
/// pack's, waiting up to [`PACK_WAIT`] while the pack is being sealed or replaced. A reader kept
/// from an earlier read that misses the batch is opened again once: an install may have replaced
/// a partial copy of the epoch since it was opened.
fn read_packs(
    packs: &PackSource,
    epoch: Epoch,
    digest: B256,
) -> Result<(Batch, BatchSource), TxBodyErrorKind> {
    let live = {
        let _permit = packs.permit();
        packs.live_batch(epoch, digest)
    };
    match live {
        Some(Ok(Some(batch))) => return Ok((batch, BatchSource::LivePack)),
        Some(Ok(None)) => return Err(TxBodyErrorKind::BatchMissing),
        // sealed under the read: read it as a closed epoch
        Some(Err(BatchReadError::Closed)) | None => {}
        Some(Err(e)) => return Err(TxBodyErrorKind::Read(Box::new(e))),
    }
    let deadline = Instant::now() + PACK_WAIT;
    let mut reopened = false;
    loop {
        let read = {
            let _permit = packs.permit();
            packs.reader(epoch).map(|(reader, fresh)| (reader.batch(digest), fresh))
        };
        match read {
            Ok((Ok(Some(batch)), _)) => return Ok((batch, BatchSource::ClosedPack)),
            Ok((Ok(None), false)) if !reopened => {
                packs.forget(epoch);
                reopened = true;
            }
            Ok((Ok(None), _)) => return Err(TxBodyErrorKind::BatchMissing),
            Ok((Err(e), _)) => return Err(TxBodyErrorKind::Read(Box::new(e))),
            Err(PackOpenError::Missing) => return Err(TxBodyErrorKind::PackMissing),
            Err(PackOpenError::Failed(e)) => return Err(TxBodyErrorKind::Read(Box::new(e))),
            Err(PackOpenError::Unsealed | PackOpenError::Installing) => {
                if Instant::now() >= deadline {
                    return Err(TxBodyErrorKind::HandoffTimeout(PACK_WAIT));
                }
                std::thread::sleep(PACK_RETRY);
            }
        }
    }
}

/// The transactions at `positions` of `batch`, each checked against the batch's length.
fn decode(
    batch: &CachedBatch,
    positions: &[u32],
) -> Result<Vec<TransactionSigned>, TxBodyErrorKind> {
    positions
        .iter()
        .map(|&pos| {
            let bytes = batch
                .tx(pos as usize)
                .ok_or(TxBodyErrorKind::PositionOutOfRange { pos, len: batch.len() })?;
            TransactionSigned::decode_2718_exact(bytes)
                .map_err(|e| TxBodyErrorKind::Decode { pos, error: e.to_string() })
        })
        .collect()
}

impl TxBodyResolver<TNPrimitives> for TxBodySource {
    fn resolve(
        &self,
        number: BlockNumber,
        header: &ExecHeader,
        positions: &[u32],
    ) -> ProviderResult<Vec<TransactionSigned>> {
        self.transactions(number, header, positions).map_err(ProviderError::other)
    }
}

/// The env's [`PersistedBatchSink`]: fills a [`TxBodySource`]'s cache with the batches each
/// persist stored by position, on a blocking task, and in test builds remembers them in its
/// [`MemoryBatches`] before returning.
#[derive(Debug)]
pub struct TxBodyCacheFill(pub(crate) Arc<TxBodySource>);

impl PersistedBatchSink for TxBodyCacheFill {
    fn batches_persisted(&self, batches: PersistedBatches) {
        #[cfg(any(test, feature = "test-utils"))]
        self.0.remember(batches.iter());
        let source = self.0.clone();
        let fill = move || source.fill_recent(batches.iter());
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(fill)),
            Err(_) => fill(),
        }
    }
}
