//! The decoded-batch cache: the batches whose transactions the `Transactions` rows of a pointer
//! datadir name by position, kept as their raw transaction bytes, keyed by batch digest.
//!
//! Two parts, each a byte-bounded LRU:
//! - **recent** ([`RECENT_SHARE`] of the budget), filled only when a persist stores a batch's
//!   transactions by position, so the batches receipt polling reads stay in memory;
//! - **cold** (the rest), filled only by pack reads, so a scan of old history evicts only other old
//!   batches.
//!
//! The key is the batch's digest, so an entry is the batch's content whatever filled it: a fill
//! for a persist that failed to commit is harmless.

use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use tn_types::{Batch, B256};

/// The decoded-batch cache's default budget, in bytes (both parts).
pub const TX_BODY_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// The recent part's share of the budget, in eighths: 56 MiB of the default 64 MiB.
const RECENT_SHARE: usize = 7;

/// The bytes an entry is charged beside its transaction bytes and offsets: the `Arc`, the key and
/// the LRU's links, roughly.
const ENTRY_OVERHEAD: usize = 64;

/// One batch's transactions as the batch carries them (EIP-2718 bytes), in one buffer.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CachedBatch {
    /// Every transaction's bytes, back to back.
    bytes: Box<[u8]>,
    /// Where each transaction ends in `bytes`.
    ends: Box<[u32]>,
}

impl CachedBatch {
    /// A copy of `batch`'s transactions. `None` when they exceed 4 GiB together, which no batch
    /// can (a batch is at most a few megabytes).
    pub(crate) fn of(batch: &Batch) -> Option<Self> {
        let total = batch.transactions.iter().map(Vec::len).sum();
        let mut bytes = Vec::with_capacity(total);
        let mut ends = Vec::with_capacity(batch.transactions.len());
        for tx in &batch.transactions {
            bytes.extend_from_slice(tx);
            ends.push(u32::try_from(bytes.len()).ok()?);
        }
        Some(Self { bytes: bytes.into_boxed_slice(), ends: ends.into_boxed_slice() })
    }

    /// How many transactions the batch holds.
    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    /// The bytes of the transaction at `position`, or `None` past the end.
    pub(crate) fn tx(&self, position: usize) -> Option<&[u8]> {
        let end = *self.ends.get(position)? as usize;
        let start = position.checked_sub(1).map_or(0, |prev| self.ends[prev] as usize);
        self.bytes.get(start..end)
    }

    /// What the entry is charged against a part's budget.
    pub(crate) fn weight(&self) -> usize {
        self.bytes.len() + 4 * self.ends.len() + ENTRY_OVERHEAD
    }
}

/// Which part of the cache answered a lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachePart {
    /// Filled at persist.
    Recent,
    /// Filled by pack reads.
    Cold,
}

impl CachePart {
    /// The metrics label.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::Cold => "cold",
        }
    }
}

/// One byte-bounded LRU part.
#[derive(Debug)]
struct Part {
    /// Which part this is.
    which: CachePart,
    /// The budget in bytes; an entry heavier than it is never kept.
    budget: usize,
    /// The entries and the bytes they are charged.
    entries: Mutex<(LruCache<B256, Arc<CachedBatch>>, usize)>,
}

impl Part {
    fn new(which: CachePart, budget: usize) -> Self {
        Self { which, budget, entries: Mutex::new((LruCache::unbounded(), 0)) }
    }

    /// The entry for `digest`, marked most recently used.
    fn get(&self, digest: &B256) -> Option<Arc<CachedBatch>> {
        self.entries.lock().0.get(digest).cloned()
    }

    /// Keep `batch` under `digest`, evicting the least recently used entries while over budget.
    fn insert(&self, digest: B256, batch: Arc<CachedBatch>) {
        let weight = batch.weight();
        if weight > self.budget {
            return;
        }
        let used = {
            let mut guard = self.entries.lock();
            let (lru, used) = &mut *guard;
            if let Some(old) = lru.put(digest, batch) {
                *used -= old.weight();
            }
            *used += weight;
            while *used > self.budget {
                match lru.pop_lru() {
                    Some((_, evicted)) => *used -= evicted.weight(),
                    None => break,
                }
            }
            *used
        };
        crate::metrics::set_tx_body_cache_bytes(self.which.label(), used);
    }

    /// The bytes the entries are charged.
    #[cfg(test)]
    fn used(&self) -> usize {
        self.entries.lock().1
    }

    /// Whether `digest` is kept, without touching its recency.
    #[cfg(test)]
    fn contains(&self, digest: &B256) -> bool {
        self.entries.lock().0.contains(digest)
    }
}

/// The decoded-batch cache ([module docs](self)).
#[derive(Debug)]
pub(crate) struct BatchCache {
    recent: Part,
    cold: Part,
}

impl BatchCache {
    /// A cache of `budget` bytes in all: seven eighths recent, the rest cold. 0 keeps nothing.
    pub(crate) fn new(budget: usize) -> Self {
        let recent = budget / 8 * RECENT_SHARE + budget % 8 * RECENT_SHARE / 8;
        Self {
            recent: Part::new(CachePart::Recent, recent),
            cold: Part::new(CachePart::Cold, budget - recent),
        }
    }

    /// The batch under `digest` and the part that holds it: recent first.
    pub(crate) fn get(&self, digest: &B256) -> Option<(Arc<CachedBatch>, CachePart)> {
        self.recent
            .get(digest)
            .map(|batch| (batch, CachePart::Recent))
            .or_else(|| self.cold.get(digest).map(|batch| (batch, CachePart::Cold)))
    }

    /// Keep a batch a persist just stored by position.
    pub(crate) fn insert_recent(&self, digest: B256, batch: Arc<CachedBatch>) {
        self.recent.insert(digest, batch);
    }

    /// Keep a batch read from a pack.
    pub(crate) fn insert_cold(&self, digest: B256, batch: Arc<CachedBatch>) {
        self.cold.insert(digest, batch);
    }

    /// The budgets of the recent and cold parts.
    #[cfg(test)]
    pub(crate) fn budgets(&self) -> (usize, usize) {
        (self.recent.budget, self.cold.budget)
    }

    /// The bytes the recent and cold parts are charged.
    #[cfg(test)]
    pub(crate) fn used(&self) -> (usize, usize) {
        (self.recent.used(), self.cold.used())
    }

    /// Whether the recent and the cold part keep `digest`.
    #[cfg(test)]
    pub(crate) fn holds(&self, digest: &B256) -> (bool, bool) {
        (self.recent.contains(digest), self.cold.contains(digest))
    }
}
