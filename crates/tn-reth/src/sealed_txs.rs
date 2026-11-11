//! Marks for pooled transactions that this node sealed into a batch that has not executed yet
//! (switch `sealed_tx_keep`, issue #1807).
//!
//! With the switch off, the batch builder removes a batch's transactions from the pool once the
//! batch reaches quorum and advances each sender's pool nonce optimistically. Until the batch
//! executes the node has no record of those transactions: a lookup by hash misses, the pending
//! nonce does not count them, and a client that resends the same bytes is accepted as new, so the
//! transaction can be sealed a second time. In bench 5 (r20261107-1504) the load generators
//! repaired these gaps after 300 s with 235,073 resend calls, 60.5 % of which the nodes took as
//! new.
//!
//! With the switch on, the builder leaves the transactions in the pool and marks them here under
//! the batch digest instead. A marked transaction is found by hash, a resend of its bytes answers
//! `AlreadyImported`, and the pending nonce counts it. The builder skips a marked transaction when
//! it selects, without `mark_invalid`, so the sender's later nonces stay buildable. Only batches
//! sealed by quorum are marked: a non-committee worker's forwarded batch is re-packed by the
//! committee under other digests, so its own digest never executes.
//!
//! A mark ends in one of four ways, each with its own counter:
//! - Execution. The canonical update for a block whose `ommers_hash` is the batch digest unmarks
//!   whatever that batch left in the pool (a transaction skipped at execution becomes buildable
//!   again), and a mined hash loses its mark whichever block mined it.
//! - Re-pool. `WorkerTxPool::add_recovered_transaction_external` unmarks the transaction it
//!   re-pools (the epoch orphan path and the forwarder requeue), and [`SealedTxs::clear_orphaned`]
//!   unmarks an orphaned batch on a node that forwards its orphans instead of re-pooling them.
//! - Eviction. A marked transaction that left the pool any other way (reth's pool limits, the
//!   queued-lifetime sweep, the nonce reload after broadcast lag) loses its mark at the next sweep,
//!   which runs with the first canonical update at least [`SEALED_TXS_SWEEP_INTERVAL`] after the
//!   previous sweep.
//! - Lag. When pool maintenance loses canonical notifications, every mark is dropped. The digests
//!   executed in the lost rounds are unknown, and a mark that outlived its batch would hold the
//!   transaction, and every later nonce of its sender, out of all future batches.
//!
//! There is no time-based expiry. The bound is the pool itself: a transaction is marked only while
//! the pool holds it, and the sweep drops the mark of a transaction the pool no longer holds, so
//! the store never holds more marks than the pool's transaction limits allow plus what left the
//! pool since the last sweep.
//!
//! The pool never reads the switch. Only the batch builder marks, and only with `sealed_tx_keep`
//! on, so with the switch off the store stays empty and every pool path behaves as before. The
//! one thing the store does regardless of the switch is remember the most recent executed digests
//! (see [`SealTicket`]).

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};
use tn_types::{BlockHash, TxHash};

/// How many executed batch digests the store remembers to refuse late marks (see
/// [`SealTicket`]).
///
/// A late mark arrives within one turn of the batch builder's run loop after its digest executed,
/// so the memory only has to span seconds. 4,096 blocks is minutes at bench rates (one block per
/// batch, tens of batches per second) and well under 1 MB.
pub const SEALED_TXS_EXECUTED_MEMORY: usize = 4_096;

/// The shortest time between two sweeps for marks whose transaction left the pool.
///
/// A sweep looks up every marked hash in the pool under one read lock. Running it on every
/// canonical update would cost one lookup per marked transaction per block; once per second keeps
/// that to a few milliseconds per second at the pool's transaction limits.
pub const SEALED_TXS_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Gauge: transactions marked now, summed over every worker pool.
const MARKED: &str = "tn_reth.sealed_txs_marked";
/// Counter: marks cleared by a canonical update (the batch's digest executed, or the hash mined).
const CLEARED_EXECUTED: &str = "tn_reth.sealed_txs_cleared_executed_total";
/// Counter: marks cleared because the transaction was re-pooled.
const CLEARED_REPOOLED: &str = "tn_reth.sealed_txs_cleared_repooled_total";
/// Counter: marks dropped because the transaction left the pool without executing.
const DROPPED_EVICTED: &str = "tn_reth.sealed_txs_dropped_evicted_total";
/// Counter: marks dropped because pool maintenance lost canonical notifications.
const CLEARED_LAG: &str = "tn_reth.sealed_txs_cleared_lag_total";
/// Counter: transactions not marked because their batch's digest executed before the mark.
const LATE_MARKS_REFUSED: &str = "tn_reth.sealed_txs_late_marks_refused_total";

/// A cheap-clone handle to one worker pool's sealed-transaction marks.
///
/// Every clone shares the same store: the batch builder marks through the pool it builds from, and
/// the pool's canonical-state maintenance task clears through its own clone.
///
/// Lock order: a caller that needs both this store and reth's pool lock takes this store first.
/// Reth never calls into the store, so the order cannot invert.
#[derive(Clone, Debug)]
pub struct SealedTxs {
    /// The shared store. A `std::sync::Mutex` because every critical section is a handful of map
    /// operations, plus at most one pool read for a mark or a sweep, and no caller awaits under
    /// it.
    inner: Arc<Mutex<SealedStore>>,
}

impl Default for SealedTxs {
    fn default() -> Self {
        Self::new()
    }
}

impl SealedTxs {
    /// Create an empty store and register its metric series at zero.
    pub fn new() -> Self {
        Self::with_memory(SEALED_TXS_EXECUTED_MEMORY)
    }

    /// Create an empty store that remembers `memory` executed digests.
    ///
    /// Private: tests use a small memory to exercise the bound, and every other caller goes
    /// through [`Self::new`].
    fn with_memory(memory: usize) -> Self {
        register_series();
        Self {
            inner: Arc::new(Mutex::new(SealedStore {
                by_digest: HashMap::new(),
                by_hash: HashMap::new(),
                generation: 0,
                lag_floor: 0,
                executed: HashMap::new(),
                executed_order: VecDeque::new(),
                memory,
                last_sweep: None,
            })),
        }
    }

    /// Lock the store, recovering from a poisoned lock: every critical section leaves both maps
    /// consistent before it can panic, so the data is still sound.
    fn store(&self) -> MutexGuard<'_, SealedStore> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take a ticket for a batch whose transaction selection has just finished.
    ///
    /// See [`SealTicket`] for when to take it.
    pub fn ticket(&self) -> SealTicket {
        SealTicket(self.store().generation)
    }

    /// Return true if `hash` is marked as sealed into a batch that has not executed.
    pub fn is_marked(&self, hash: &TxHash) -> bool {
        self.store().by_hash.contains_key(hash)
    }

    /// Return the digest of the batch that `hash` is marked under, if any.
    pub fn digest_of(&self, hash: &TxHash) -> Option<BlockHash> {
        self.store().by_hash.get(hash).copied()
    }

    /// The number of marked transactions.
    pub fn len(&self) -> usize {
        self.store().by_hash.len()
    }

    /// Return true if nothing is marked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark the transactions of the batch `digest` that the pool still holds, and return how many
    /// were newly marked.
    ///
    /// `present` receives the candidate hashes and returns those the pool holds; it runs under the
    /// store lock, so a canonical update cannot remove a transaction between that lookup and its
    /// mark. If `digest` executed after `ticket` was taken, or canonical notifications were lost
    /// after it was taken, nothing is marked and the transactions are counted as refused late
    /// marks (see [`SealTicket`]).
    ///
    /// A hash already marked under `digest` is not counted again. A hash marked under another
    /// digest moves to `digest`.
    pub(crate) fn mark_present(
        &self,
        ticket: SealTicket,
        digest: BlockHash,
        hashes: &[TxHash],
        present: impl FnOnce(Vec<TxHash>) -> Vec<TxHash>,
    ) -> usize {
        let mut store = self.store();
        if store.executed_after(&digest, ticket) || ticket.0 < store.lag_floor {
            metrics::counter!(LATE_MARKS_REFUSED).increment(as_u64(hashes.len()));
            0
        } else {
            let marked = present(hashes.to_vec())
                .into_iter()
                .filter(|hash| store.insert(digest, *hash))
                .count();
            metrics::gauge!(MARKED).increment(marked as f64);
            marked
        }
    }

    /// Apply one canonical update: remember each executed `digest`, unmark everything still marked
    /// under it, then drop the mark of every `mined` hash. Return the number of marks cleared.
    ///
    /// The caller runs this after the pool update has removed the mined transactions, so the pool
    /// never holds a mined transaction unmarked. A builder walking an older snapshot of the pool
    /// can still meet one; it reads the mark first and then asks the pool whether it still holds
    /// the transaction.
    pub(crate) fn clear_executed(
        &self,
        digests: impl IntoIterator<Item = BlockHash>,
        mined: impl IntoIterator<Item = TxHash>,
    ) -> usize {
        let mut store = self.store();
        store.generation = store.generation.saturating_add(1);
        let generation = store.generation;
        let by_digest: usize = digests
            .into_iter()
            .map(|digest| {
                store.remember_executed(digest, generation);
                store.unmark_digest(&digest)
            })
            .sum();
        // the digest pass already cleared this node's own batches; a hash left here was mined
        // under another digest (a peer's copy of the same transaction)
        let by_hash = if store.by_hash.is_empty() {
            0
        } else {
            mined.into_iter().filter(|hash| store.remove(hash)).count()
        };
        let cleared = by_digest.saturating_add(by_hash);
        record_cleared(CLEARED_EXECUTED, cleared);
        cleared
    }

    /// Unmark `hash` because it is being re-pooled, and return true if it was marked.
    pub(crate) fn clear_repooled(&self, hash: &TxHash) -> bool {
        let cleared = self.store().remove(hash);
        if cleared {
            record_cleared(CLEARED_REPOOLED, 1);
        }
        cleared
    }

    /// Unmark everything marked under the batch `digest` because the batch is orphaned, and
    /// return how many marks were cleared (counted with the re-pool clears).
    ///
    /// An orphaned batch never executes, so no canonical update will clear its marks. The epoch
    /// orphan path re-pools a batch's transactions through
    /// `WorkerTxPool::add_recovered_transaction_external`, which clears each mark, but only on a
    /// node that stays in the committee; a node that leaves it forwards the orphan instead and
    /// must clear the digest here, for every pool.
    ///
    /// The digest is also remembered as if it executed now, so a mark for it whose ticket
    /// predates this call is refused as late (see [`SealTicket`]). The worker can ack such a
    /// batch during the closing epoch's shutdown grace and its mark can run after this pass;
    /// nothing would clear that mark later.
    pub fn clear_orphaned(&self, digest: &BlockHash) -> usize {
        let mut store = self.store();
        store.generation = store.generation.saturating_add(1);
        let generation = store.generation;
        store.remember_executed(*digest, generation);
        let cleared = store.unmark_digest(digest);
        drop(store);
        record_cleared(CLEARED_REPOOLED, cleared);
        cleared
    }

    /// Return true, and start a new interval, if a sweep is due at `now` and anything is marked.
    pub(crate) fn sweep_due(&self, now: Instant) -> bool {
        let mut store = self.store();
        let due = !store.by_hash.is_empty()
            && store.last_sweep.is_none_or(|last| {
                now.saturating_duration_since(last) >= SEALED_TXS_SWEEP_INTERVAL
            });
        if due {
            store.last_sweep = Some(now);
        }
        due
    }

    /// Drop the mark of every transaction the pool no longer holds, and return how many were
    /// dropped.
    ///
    /// `present` receives every marked hash and returns those the pool holds; it runs under the
    /// store lock, so a mark made meanwhile cannot be dropped by a lookup that predates it.
    pub(crate) fn drop_missing(&self, present: impl FnOnce(Vec<TxHash>) -> Vec<TxHash>) -> usize {
        let mut store = self.store();
        if store.by_hash.is_empty() {
            0
        } else {
            let held: HashSet<TxHash> =
                present(store.by_hash.keys().copied().collect()).into_iter().collect();
            // the order of `gone` does not matter: each hash is removed independently
            let gone: Vec<TxHash> =
                store.by_hash.keys().filter(|hash| !held.contains(*hash)).copied().collect();
            let dropped = gone.iter().filter(|hash| store.remove(hash)).count();
            record_cleared(DROPPED_EVICTED, dropped);
            dropped
        }
    }

    /// Drop every mark because canonical notifications were lost, and return how many were
    /// dropped.
    ///
    /// The lost notifications carried digests that are now in neither the executed memory nor
    /// the generation count, so a batch whose ticket predates the lag may already have executed.
    /// Raising the lag floor refuses every such ticket: at most each worker's one in-flight batch
    /// goes unmarked, and its transactions may be sealed twice (execution skips the copy). The
    /// executed-digest memory is kept: it only ever refuses marks.
    pub(crate) fn clear_all_lag(&self) -> usize {
        let mut store = self.store();
        store.generation = store.generation.saturating_add(1);
        store.lag_floor = store.generation;
        let cleared = store.by_hash.len();
        store.by_hash.clear();
        store.by_digest.clear();
        record_cleared(CLEARED_LAG, cleared);
        cleared
    }
}

/// Proof of when a batch's transaction selection finished, used to refuse a late mark.
///
/// The builder marks a batch when the worker acks its quorum, and the worker acks only after it
/// has reported the digest to the primary. Under load the batch can be proposed, certified,
/// committed and executed before the builder's run loop handles that ack. Marking then would
/// leave whatever the block skipped (a nonce-gapped transaction) marked with no execution left to
/// clear it.
///
/// The store counts canonical updates and remembers, for the last
/// [`SEALED_TXS_EXECUTED_MEMORY`] executed digests, the count at which each executed. A ticket is
/// the count when it was taken. Take it after the batch's selection has finished and before the
/// batch goes to the worker: any execution of this batch comes after the report, so after the
/// ticket, and a mark against such a digest is refused. An identical earlier batch (same
/// transactions, same digest) that executed with every transaction skipped executed before this
/// selection could see its transactions unmarked, so before the ticket, and does not refuse the
/// new mark.
///
/// Lost canonical notifications also count as a canonical update and raise a floor: a ticket
/// taken before the loss is refused, because the batch may have executed in the lost rounds.
/// An epoch's orphan pass counts as one too, and remembers each orphaned digest as executed, so
/// a ticket for that digest taken before the pass is refused.
///
/// A ticket only compares against the store that issued it: take it from the pool the batch's
/// transactions came from, and mark through that same pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SealTicket(u64);

/// The store's state.
///
/// Invariant: `by_hash[h] == d` exactly when `h` is in `by_digest[d]`, and no set in `by_digest`
/// is empty. Every method keeps both maps in step before it returns.
#[derive(Debug)]
struct SealedStore {
    /// Marked hashes by the digest of the batch they were sealed into.
    by_digest: HashMap<BlockHash, HashSet<TxHash>>,
    /// The digest each marked hash was sealed under.
    by_hash: HashMap<TxHash, BlockHash>,
    /// The number of canonical updates applied so far, lag events and orphaned digests
    /// included.
    generation: u64,
    /// The generation of the last lag event: a ticket below it is refused.
    lag_floor: u64,
    /// The generation at which each remembered digest last executed or was orphaned.
    executed: HashMap<BlockHash, u64>,
    /// The remembered executions in order, oldest first, bounding `executed`.
    executed_order: VecDeque<(BlockHash, u64)>,
    /// The bound on `executed_order`.
    memory: usize,
    /// When the last sweep for missing transactions ran.
    last_sweep: Option<Instant>,
}

impl SealedStore {
    /// Mark `hash` under `digest`; return true if it was not marked before.
    fn insert(&mut self, digest: BlockHash, hash: TxHash) -> bool {
        match self.by_hash.insert(hash, digest) {
            Some(previous) if previous == digest => false,
            Some(previous) => {
                self.unlink(&previous, &hash);
                self.by_digest.entry(digest).or_default().insert(hash);
                false
            }
            None => {
                self.by_digest.entry(digest).or_default().insert(hash);
                true
            }
        }
    }

    /// Unmark `hash`; return true if it was marked.
    fn remove(&mut self, hash: &TxHash) -> bool {
        self.by_hash.remove(hash).map(|digest| self.unlink(&digest, hash)).is_some()
    }

    /// Remove `hash` from `digest`'s set, dropping the set once it is empty.
    fn unlink(&mut self, digest: &BlockHash, hash: &TxHash) {
        if let Entry::Occupied(mut entry) = self.by_digest.entry(*digest) {
            entry.get_mut().remove(hash);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    /// Unmark everything marked under `digest`; return how many marks were removed.
    fn unmark_digest(&mut self, digest: &BlockHash) -> usize {
        let hashes = self.by_digest.remove(digest).unwrap_or_default();
        hashes.iter().for_each(|hash| {
            self.by_hash.remove(hash);
        });
        hashes.len()
    }

    /// Remember that `digest` executed at `generation`, forgetting the oldest execution past the
    /// memory bound.
    fn remember_executed(&mut self, digest: BlockHash, generation: u64) {
        self.executed.insert(digest, generation);
        self.executed_order.push_back((digest, generation));
        while self.executed_order.len() > self.memory {
            if let Some((oldest, at)) = self.executed_order.pop_front() {
                // a digest that executed again later keeps its newer entry
                if self.executed.get(&oldest) == Some(&at) {
                    self.executed.remove(&oldest);
                }
            }
        }
    }

    /// Return true if `digest` executed after `ticket` was taken.
    fn executed_after(&self, digest: &BlockHash, ticket: SealTicket) -> bool {
        self.executed.get(digest).is_some_and(|at| *at > ticket.0)
    }
}

impl Drop for SealedStore {
    /// Take this store's marks off the shared gauge when the last handle goes (a retired pool).
    fn drop(&mut self) {
        metrics::gauge!(MARKED).decrement(self.by_hash.len() as f64);
    }
}

/// Register every series at zero, so a node that never marks still exports them and the first
/// event renders as a step from zero.
fn register_series() {
    metrics::gauge!(MARKED).increment(0.0);
    [CLEARED_EXECUTED, CLEARED_REPOOLED, DROPPED_EVICTED, CLEARED_LAG, LATE_MARKS_REFUSED]
        .into_iter()
        .for_each(|name| metrics::counter!(name).increment(0));
}

/// Count `cleared` marks under the counter `name` and take them off the gauge.
fn record_cleared(name: &'static str, cleared: usize) {
    if cleared > 0 {
        metrics::counter!(name).increment(as_u64(cleared));
        metrics::gauge!(MARKED).decrement(cleared as f64);
    }
}

/// Convert a count for a counter, saturating instead of truncating.
fn as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// A distinct transaction hash.
    fn tx(byte: u8) -> TxHash {
        TxHash::repeat_byte(byte)
    }

    /// A distinct batch digest.
    fn digest(byte: u8) -> BlockHash {
        BlockHash::repeat_byte(byte)
    }

    /// A pool that holds every candidate.
    fn all(hashes: Vec<TxHash>) -> Vec<TxHash> {
        hashes
    }

    /// Mark with a fresh ticket against a pool that holds every candidate.
    fn mark(store: &SealedTxs, batch: BlockHash, hashes: &[TxHash]) -> usize {
        store.mark_present(store.ticket(), batch, hashes, all)
    }

    /// Every series of `recorder` by name, from ONE snapshot: a snapshot resets counters and
    /// gauges, so reading each series from its own snapshot would see zeros after the first.
    fn snapshot(recorder: &DebuggingRecorder) -> HashMap<String, DebugValue> {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, .., value)| (key.key().name().to_string(), value))
            .collect()
    }

    #[test]
    fn marks_only_what_the_pool_holds_and_counts_each_once() {
        let store = SealedTxs::new();
        let held = [tx(1), tx(2)];
        let marked = store.mark_present(store.ticket(), digest(9), &[tx(1), tx(2), tx(3)], |c| {
            c.into_iter().filter(|hash| held.contains(hash)).collect()
        });
        assert_eq!(marked, 2);
        assert!(store.is_marked(&tx(1)) && store.is_marked(&tx(2)));
        assert!(!store.is_marked(&tx(3)), "a hash the pool does not hold is never marked");
        assert_eq!(mark(&store, digest(9), &[tx(1), tx(2)]), 0, "a repeated ack marks nothing new");
        assert_eq!(store.len(), 2);
        assert_eq!(store.digest_of(&tx(1)), Some(digest(9)));
    }

    #[test]
    fn a_hash_marked_again_moves_to_the_new_digest() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1), tx(2)]);
        assert_eq!(mark(&store, digest(2), &[tx(2)]), 0, "a move is not a new mark");
        assert_eq!(store.digest_of(&tx(2)), Some(digest(2)));
        // executing the old digest leaves the moved hash marked
        assert_eq!(store.clear_executed([digest(1)], []), 1);
        assert!(store.is_marked(&tx(2)));
        assert_eq!(store.clear_executed([digest(2)], []), 1);
        assert!(store.is_empty());
    }

    #[test]
    fn execution_clears_its_digest_and_every_mined_hash_only() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1), tx(2)]);
        mark(&store, digest(2), &[tx(3), tx(4)]);
        // block for digest 1 skipped tx 2, and another node's block mined tx 3
        assert_eq!(store.clear_executed([digest(1)], [tx(1), tx(3)]), 3);
        assert!(!store.is_marked(&tx(2)), "a transaction its own batch skipped becomes buildable");
        assert!(!store.is_marked(&tx(3)), "a mined hash loses its mark under any digest");
        assert!(store.is_marked(&tx(4)), "another digest's unmined mark stays");
        assert_eq!(
            store.clear_executed([digest(7)], [tx(9)]),
            0,
            "unrelated updates clear nothing"
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn repool_clears_one_mark() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1), tx(2)]);
        assert!(store.clear_repooled(&tx(1)));
        assert!(!store.clear_repooled(&tx(1)), "a second re-pool finds no mark");
        assert!(!store.is_marked(&tx(1)));
        assert!(store.is_marked(&tx(2)));
    }

    #[test]
    fn sweep_drops_marks_the_pool_no_longer_holds() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1), tx(2), tx(3)]);
        let dropped = store.drop_missing(|marked| {
            assert_eq!(marked.len(), 3, "the sweep looks up every marked hash");
            marked.into_iter().filter(|hash| *hash != tx(2)).collect()
        });
        assert_eq!(dropped, 1);
        assert!(!store.is_marked(&tx(2)));
        assert_eq!(store.len(), 2);
        assert_eq!(store.drop_missing(all), 0);
    }

    #[test]
    fn sweep_runs_at_most_once_per_interval_and_never_when_empty() {
        let store = SealedTxs::new();
        let start = Instant::now();
        assert!(!store.sweep_due(start), "an empty store never sweeps");
        mark(&store, digest(1), &[tx(1)]);
        assert!(store.sweep_due(start));
        assert!(!store.sweep_due(start + SEALED_TXS_SWEEP_INTERVAL / 2));
        assert!(store.sweep_due(start + SEALED_TXS_SWEEP_INTERVAL));
    }

    #[test]
    fn an_orphaned_digest_is_cleared_whole() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1), tx(2)]);
        mark(&store, digest(2), &[tx(3)]);
        assert_eq!(store.clear_orphaned(&digest(1)), 2);
        assert_eq!(store.clear_orphaned(&digest(1)), 0);
        assert!(store.is_marked(&tx(3)));
        assert_eq!(store.len(), 1);
    }

    /// The batch may have executed in the lost rounds, which neither the executed memory nor the
    /// generation count saw: a ticket taken before the lag is refused, one taken after is not.
    #[test]
    fn lag_refuses_tickets_taken_before_it() {
        let store = SealedTxs::new();
        let before = store.ticket();
        store.clear_all_lag();
        assert_eq!(store.mark_present(before, digest(1), &[tx(1)], all), 0);
        let after = store.ticket();
        assert_eq!(store.mark_present(after, digest(1), &[tx(1)], all), 1);
    }

    #[test]
    fn lag_clears_every_mark_but_keeps_the_executed_memory() {
        let store = SealedTxs::new();
        let ticket = store.ticket();
        store.clear_executed([digest(5)], []);
        mark(&store, digest(1), &[tx(1)]);
        mark(&store, digest(2), &[tx(2)]);
        assert_eq!(store.clear_all_lag(), 2);
        assert!(store.is_empty());
        assert_eq!(store.mark_present(ticket, digest(5), &[tx(3)], all), 0);
    }

    /// The late-ack race: the digest executed after selection, before the builder marked it.
    #[test]
    fn a_mark_after_its_digest_executed_is_refused() {
        let store = SealedTxs::new();
        let ticket = store.ticket();
        // the block for digest 1 executes and skips tx 2 before the ack is handled
        store.clear_executed([digest(1)], [tx(1)]);
        assert_eq!(store.mark_present(ticket, digest(1), &[tx(1), tx(2)], all), 0);
        assert!(!store.is_marked(&tx(2)), "nothing would ever clear this mark");
        // a different batch sealed under the same ticket is unaffected
        assert_eq!(store.mark_present(ticket, digest(2), &[tx(3)], all), 1);
    }

    /// An identical batch re-sealed after its first copy executed has the same digest; a ticket
    /// taken after that execution must not mistake it for a late ack.
    #[test]
    fn an_identical_reseal_after_execution_is_marked() {
        let store = SealedTxs::new();
        mark(&store, digest(1), &[tx(1)]);
        // first copy executes and skips tx 1 (nonce too high): unmarked, still pooled
        assert_eq!(store.clear_executed([digest(1)], []), 1);
        let ticket = store.ticket();
        assert_eq!(store.mark_present(ticket, digest(1), &[tx(1)], all), 1);
        // its own later execution still clears it
        assert_eq!(store.clear_executed([digest(1)], []), 1);
        assert!(store.is_empty());
    }

    #[test]
    fn executed_memory_is_bounded_and_keeps_the_newest() {
        let store = SealedTxs::with_memory(2);
        let ticket = store.ticket();
        store.clear_executed([digest(1)], []);
        store.clear_executed([digest(2)], []);
        store.clear_executed([digest(1)], []);
        store.clear_executed([digest(3)], []);
        let memory = store.store().executed.len();
        assert_eq!(memory, 2, "only the newest two executions are remembered");
        assert_eq!(store.mark_present(ticket, digest(2), &[tx(2)], all), 1, "digest 2 forgotten");
        assert_eq!(store.mark_present(ticket, digest(1), &[tx(1)], all), 0, "re-execution kept");
        assert_eq!(store.mark_present(ticket, digest(3), &[tx(3)], all), 0);
    }

    #[test]
    fn metrics_count_every_way_a_mark_ends() {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let store = SealedTxs::new();
            let late = store.ticket();
            store.clear_executed([digest(8)], []);
            assert_eq!(store.mark_present(late, digest(8), &[tx(8), tx(9)], all), 0);
            mark(&store, digest(1), &[tx(1), tx(2), tx(3), tx(4), tx(5)]);
            store.clear_executed([digest(1)], []);
            mark(&store, digest(2), &[tx(1), tx(2), tx(3), tx(4), tx(5)]);
            store.clear_executed([], [tx(1)]);
            store.clear_repooled(&tx(2));
            store.drop_missing(|marked| marked.into_iter().filter(|hash| *hash != tx(3)).collect());
            store.clear_all_lag();
            mark(&store, digest(3), &[tx(6)]);
        });
        let series = snapshot(&recorder);
        assert_eq!(series.get(CLEARED_EXECUTED), Some(&DebugValue::Counter(6)));
        assert_eq!(series.get(CLEARED_REPOOLED), Some(&DebugValue::Counter(1)));
        assert_eq!(series.get(DROPPED_EVICTED), Some(&DebugValue::Counter(1)));
        assert_eq!(series.get(CLEARED_LAG), Some(&DebugValue::Counter(2)));
        assert_eq!(series.get(LATE_MARKS_REFUSED), Some(&DebugValue::Counter(2)));
        // the store was dropped inside the closure with one mark left: the gauge returns to zero
        assert_eq!(series.get(MARKED), Some(&DebugValue::Gauge(0.0.into())));
    }

    #[test]
    fn marked_gauge_tracks_live_marks() {
        let recorder = DebuggingRecorder::new();
        let store = metrics::with_local_recorder(&recorder, || {
            let store = SealedTxs::new();
            mark(&store, digest(1), &[tx(1), tx(2), tx(3)]);
            store.clear_repooled(&tx(1));
            store
        });
        assert_eq!(snapshot(&recorder).get(MARKED), Some(&DebugValue::Gauge(2.0.into())));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn series_exist_at_zero_before_any_event() {
        let recorder = DebuggingRecorder::new();
        let store = metrics::with_local_recorder(&recorder, SealedTxs::new);
        assert!(store.is_empty());
        let series = snapshot(&recorder);
        [CLEARED_EXECUTED, CLEARED_REPOOLED, DROPPED_EVICTED, CLEARED_LAG, LATE_MARKS_REFUSED]
            .into_iter()
            .for_each(|name| assert_eq!(series.get(name), Some(&DebugValue::Counter(0))));
        assert_eq!(series.get(MARKED), Some(&DebugValue::Gauge(0.0.into())));
    }

    /// L1 of the review: a batch acked during the closing epoch's shutdown grace whose mark
    /// runs only after the next epoch's orphan pass cleared its digest. The digest never
    /// executes and the pass has run, so nothing would ever clear such a mark: the pass refuses
    /// it as a late mark. Another digest's ticket of the same age still marks, and an identical
    /// batch selected after the pass (a same-epoch re-entry) is marked normally.
    #[test]
    fn a_mark_after_its_digest_was_orphaned_is_refused() {
        let store = SealedTxs::new();
        let late = store.ticket();
        assert_eq!(store.clear_orphaned(&digest(1)), 0);

        assert_eq!(store.mark_present(late, digest(1), &[tx(1), tx(2)], all), 0);
        assert!(store.is_empty());
        assert_eq!(store.mark_present(late, digest(2), &[tx(3)], all), 1);
        assert_eq!(mark(&store, digest(1), &[tx(1), tx(2)]), 2);
    }
}
