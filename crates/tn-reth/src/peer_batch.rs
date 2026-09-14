//! Short-lived memory of transactions already packed by a validated peer batch.
//!
//! A client can submit one signed transaction to every committee validator's RPC. Each worker
//! packs it into its own batch, every batch passes peer validation, and only the first executed
//! copy pays: the later copies are skipped for free at execution. The duplicates still consume
//! batch space, bandwidth, and vote rounds on every worker lane.
//!
//! This window is the local, protocol-neutral half of the fix. When a node validates a peer's
//! batch it remembers that batch's transaction hashes for a bounded time, and its own batch
//! builder defers those hashes (and, through `mark_invalid`, the sender's later nonces, which
//! could not execute ahead of the deferred nonce anyway, so they wait exactly as long as it
//! does) instead of packing a copy. No peer is penalized and nothing leaves the pool: after the
//! peer batch executes, the canonical update drops the transaction from the pool anyway, so the
//! memory only matters while the peer batch is in flight or lost. A build in which every pending
//! transaction is deferred seals nothing at all (`BuildOutcome::Empty` in
//! `tn-batch-builder`), because an empty batch is a message peers reject and penalize as fatal.

use metrics::{Counter, Gauge};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use tn_types::{TxHash, WorkerId};

/// How long a transaction stays deferred after a peer batch that carries it validates.
///
/// This equals the default `batch_vote_timeout` (`crates/config/src/node.rs`). A peer batch that
/// has not gathered its quorum by then has been abandoned by its producer, and one that has is on
/// its way into a header.
///
/// Each arming defers a transaction for at most one TTL. A fresh arming is only possible after
/// the entry is forgotten at twice the TTL (see [`PeerBatchWindow::forget_expired`]), and during
/// the immune half this node's builder packs the transaction if it is still pending. A byzantine
/// peer that keeps reporting a batch it never certifies therefore adds at most one TTL of delay
/// per two TTLs and cannot censor.
///
/// An honest producer that misses its vote quorum rebuilds the same transactions and re-reports
/// the same digest; that repeat report lands inside the immune window and is a no-op.
pub const PEER_BATCH_DEFER_TTL: Duration = Duration::from_secs(10);

/// Hard cap on the number of remembered transaction hashes.
///
/// Entries are never evicted early. While the window holds this many live entries a further hash
/// is not remembered at all, and the builder treats that transaction exactly as it did before
/// this window existed (it packs it). Hashes become recordable again only as entries age out at
/// twice the TTL.
///
/// Evicting to make room would be a censorship lever. Batch validation checks neither balance nor
/// nonce, so a byzantine validator can push this many junk hashes through structurally valid
/// batches, evict a target's immune entry, and re-arm the target on every cycle: unbounded
/// deferral of one sender on every honest builder. With the drop-when-full policy a flood of peer
/// batches can only switch the deferral off, never re-arm an entry.
///
/// Memory bound: about 100 bytes per entry (a 32-byte hash plus an `Instant` in both the map and
/// the order queue, plus hash-map overhead), so under 7 MB at the cap.
///
/// Honest load: 65,536 hashes is about 46 full batches of 21,000-gas transfers (`max_batch_gas`
/// is 30M), or twenty seconds of peer batches at 3,276 transactions per second, which is longer
/// than the two TTLs any entry survives.
pub const PEER_BATCH_SEEN_MAX_TXS: usize = 65_536;

/// A cheap-clone handle to the shared deferral window.
///
/// Cloning shares the same window: the batch validator records into the clone held by the pool,
/// and the batch builder reads it through the pool it builds from.
#[derive(Clone, Debug)]
pub struct PeerBatchTxs {
    /// The shared window. A `std::sync::Mutex` (not an async lock) because every critical
    /// section is a handful of map operations and the builder is a synchronous function.
    inner: Arc<Mutex<PeerBatchWindow>>,
}

/// The window's state.
///
/// `order` mirrors `seen` in insertion order, which equals time order because an existing entry
/// is never refreshed or re-timestamped and the clock is always sampled under the lock.
#[derive(Debug)]
struct PeerBatchWindow {
    /// How long an entry defers its transaction.
    ttl: Duration,
    /// Upper bound on `seen.len()`.
    cap: usize,
    /// The remembered hashes with the instant each was first recorded.
    seen: HashMap<TxHash, Instant>,
    /// The same entries in insertion (time) order, oldest at the front.
    order: VecDeque<(Instant, TxHash)>,
    /// Telemetry registered by the validator before it accepts peer batches.
    metrics: PeerBatchMetrics,
}

/// Whether an attempted insertion retained a hash or lost deferral coverage.
enum PeerBatchInsertion {
    /// The hash was newly remembered.
    Remembered,
    /// The hash already has an entry, including the immune half of its lifetime.
    AlreadyRemembered,
    /// The hash was unknown and the bounded window had no room for it.
    AtCapacity,
}

/// Handles shared by worker label, with one occupancy contribution per live window.
struct PeerBatchMetrics {
    /// Retained hashes, including immune entries and expired entries awaiting the next record.
    retained_hashes: Gauge,
    /// Unknown-hash insertion attempts dropped at capacity, including repeated attempts.
    insertions_dropped_total: Counter,
}

impl std::fmt::Debug for PeerBatchMetrics {
    /// Format the handles without requiring the recorder's implementation to implement `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerBatchMetrics").finish_non_exhaustive()
    }
}

impl Default for PeerBatchMetrics {
    /// Standalone windows remain unregistered until a validator assigns their worker label.
    fn default() -> Self {
        Self { retained_hashes: Gauge::noop(), insertions_dropped_total: Counter::noop() }
    }
}

impl PeerBatchMetrics {
    /// Convert a retained count to an exact gauge input; the per-window cap fits in `u32`.
    fn retained_value(count: usize) -> f64 {
        f64::from(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// Register the bounded worker label, including a zero counter before any overflow occurs.
    fn for_worker(worker_id: WorkerId) -> Self {
        let metrics = Self {
            retained_hashes: metrics::gauge!(
                "tn_peer_batch.retained_hashes", "worker" => worker_id.to_string()
            ),
            insertions_dropped_total: metrics::counter!(
                "tn_peer_batch.insertions_dropped_total", "worker" => worker_id.to_string()
            ),
        };
        metrics.insertions_dropped_total.increment(0);
        metrics
    }
}

impl Drop for PeerBatchWindow {
    /// Release only this window's contribution, including across overlapping epoch lifetimes.
    fn drop(&mut self) {
        self.metrics.retained_hashes.decrement(PeerBatchMetrics::retained_value(self.seen.len()));
    }
}

impl Default for PeerBatchTxs {
    fn default() -> Self {
        Self::new(PEER_BATCH_DEFER_TTL)
    }
}

impl PeerBatchTxs {
    /// Create an empty window that defers a recorded hash for `ttl`.
    pub fn new(ttl: Duration) -> Self {
        Self::with_cap(ttl, PEER_BATCH_SEEN_MAX_TXS)
    }

    /// Register this window's telemetry under `worker_id` before validating peer batches.
    ///
    /// Clones share one occupancy contribution. Re-registration and overlapping old/new epoch
    /// windows preserve the total retained hashes for the worker; counters keep accumulating.
    /// Occupancy includes immune entries and expired entries until the next record prunes them.
    pub fn register_metrics(&self, worker_id: WorkerId) {
        let mut window = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let retained = window.seen.len();
        window.metrics.retained_hashes.decrement(PeerBatchMetrics::retained_value(retained));
        window.metrics = PeerBatchMetrics::for_worker(worker_id);
        window.metrics.retained_hashes.increment(PeerBatchMetrics::retained_value(retained));
    }

    /// Create an empty window with an explicit capacity.
    ///
    /// Private: tests use a small capacity to exercise the full-window policy without recording
    /// 65,536 hashes, and every other caller goes through [`Self::new`].
    fn with_cap(ttl: Duration, cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PeerBatchWindow {
                ttl,
                cap,
                seen: HashMap::new(),
                order: VecDeque::new(),
                metrics: PeerBatchMetrics::default(),
            })),
        }
    }

    /// Remember `hashes` as packed by a validated peer batch, as of now.
    ///
    /// The clock is sampled after the lock is taken, not before. Batch validation runs on many
    /// network handler tasks at once, and sampling first would let a recorder that lost the race
    /// push a decreasing `Instant` into `order`, which the `take_while` prune in
    /// [`PeerBatchWindow::forget_expired`] reads as sorted.
    pub fn record(&self, hashes: &[TxHash]) {
        let mut window = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        window.record(hashes, now);
    }

    /// Remember `hashes` as packed by a validated peer batch, as of `now`.
    ///
    /// A hash already in the window keeps its original timestamp, so a peer that re-reports the
    /// same batch cannot extend the deferral, and an entry whose deferral has expired stays
    /// immune to re-arming until it is forgotten at twice the TTL. Each arming therefore costs
    /// the transaction at most one TTL, and buying another one costs the reporter a full TTL in
    /// which this node's builder packs the transaction if it is still pending.
    #[cfg(test)]
    fn record_at(&self, hashes: &[TxHash], now: Instant) {
        let mut window = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        window.record(hashes, now);
    }

    /// Return true if `hash` was recorded by a validated peer batch within the TTL, as of now.
    pub fn is_deferred(&self, hash: &TxHash) -> bool {
        let window = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        window.is_deferred(hash, now)
    }

    /// Return true if `hash` was recorded by a validated peer batch within the TTL, as of `now`.
    #[cfg(test)]
    fn is_deferred_at(&self, hash: &TxHash, now: Instant) -> bool {
        let window = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        window.is_deferred(hash, now)
    }

    /// The number of remembered hashes, including expired entries not yet forgotten.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).seen.len()
    }

    /// Return true if nothing is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PeerBatchWindow {
    /// Forget what has aged out, then remember every hash that still fits.
    fn record(&mut self, hashes: &[TxHash], now: Instant) {
        let retained_before = self.seen.len();
        self.forget_expired(now);
        let dropped = hashes.iter().fold(0_u64, |dropped, hash| match self.insert(*hash, now) {
            PeerBatchInsertion::Remembered | PeerBatchInsertion::AlreadyRemembered => dropped,
            PeerBatchInsertion::AtCapacity => dropped.saturating_add(1),
        });
        self.metrics.insertions_dropped_total.increment(dropped);
        self.metrics.retained_hashes.increment(PeerBatchMetrics::retained_value(
            self.seen.len().saturating_sub(retained_before),
        ));
        self.metrics.retained_hashes.decrement(PeerBatchMetrics::retained_value(
            retained_before.saturating_sub(self.seen.len()),
        ));
    }

    /// Return true if `hash` is remembered and its deferral has not elapsed at `now`.
    fn is_deferred(&self, hash: &TxHash, now: Instant) -> bool {
        let ttl = self.ttl;
        self.seen.get(hash).is_some_and(|seen_at| now.duration_since(*seen_at) < ttl)
    }

    /// Drop entries older than twice the TTL.
    ///
    /// Twice the TTL, not the TTL itself, is what makes an expired entry immune to re-arming for
    /// one more TTL: a peer that keeps reporting the same batch cannot chain deferrals.
    fn forget_expired(&mut self, now: Instant) {
        let forget_after = self.ttl.saturating_mul(2);
        let stale = self
            .order
            .iter()
            .take_while(|(seen_at, _)| now.duration_since(*seen_at) >= forget_after)
            .count();
        let seen = &mut self.seen;
        self.order.drain(..stale).for_each(|(_, hash)| {
            seen.remove(&hash);
        });
    }

    /// Record `hash` at `now` unless it is already remembered or the window is full.
    ///
    /// A full window drops the hash instead of evicting a live entry, which is what stops a
    /// flood from re-arming a target's deferral (see [`PEER_BATCH_SEEN_MAX_TXS`]).
    fn insert(&mut self, hash: TxHash, now: Instant) -> PeerBatchInsertion {
        if self.seen.contains_key(&hash) {
            PeerBatchInsertion::AlreadyRemembered
        } else if self.seen.len() >= self.cap {
            PeerBatchInsertion::AtCapacity
        } else {
            self.seen.insert(hash, now);
            self.order.push_back((now, hash));
            PeerBatchInsertion::Remembered
        }
    }
}

#[cfg(test)]
mod metrics_tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Observe capacity loss without counting remembered or immune hashes as dropped insertions.
    #[test]
    fn telemetry_distinguishes_capacity_loss_from_repeated_hashes() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let sample = |name: &str| {
            snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
                (key.key().name() == name
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "worker" && label.value() == "7"))
                .then_some(value)
            })
        };
        let occupancy = "tn_peer_batch.retained_hashes";
        let dropped = "tn_peer_batch.insertions_dropped_total";
        let first = TxHash::from([1; 32]);
        let second = TxHash::from([2; 32]);
        let overflow = TxHash::from([3; 32]);
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;

        metrics::with_local_recorder(&recorder, || {
            let window = PeerBatchTxs::with_cap(ttl, 2);
            window.register_metrics(7);
            assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 0.0));
            assert!(matches!(sample(dropped), Some(DebugValue::Counter(0))));

            window.record_at(&[first, second], t0);
            window.record_at(&[first, overflow, overflow], t0 + Duration::from_secs(1));
            assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 2.0));
            assert!(matches!(sample(dropped), Some(DebugValue::Counter(2))));
            assert!(!window.is_deferred_at(&overflow, t0 + Duration::from_secs(1)));

            window.record_at(&[first], t0 + ttl);
            assert!(!window.is_deferred_at(&first, t0 + ttl));
            assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 2.0));
            assert!(matches!(sample(dropped), Some(DebugValue::Counter(2))));

            window.record_at(&[], t0 + ttl * 2);
            assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 0.0));
            window.record_at(&[overflow], t0 + ttl * 2);
            assert!(window.is_deferred_at(&overflow, t0 + ttl * 2));
            assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 1.0));
            assert!(matches!(sample(dropped), Some(DebugValue::Counter(2))));
        });
        assert!(matches!(sample(occupancy), Some(DebugValue::Gauge(value)) if value.0 == 0.0));
    }

    /// Clones, repeated registration, and overlapping epoch windows do not overwrite occupancy.
    #[test]
    fn telemetry_tracks_window_lifetimes_and_worker_labels() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let occupancy = |worker: &str| {
            snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
                (key.key().name() == "tn_peer_batch.retained_hashes"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "worker" && label.value() == worker))
                .then_some(value)
            })
        };
        let t0 = Instant::now();
        metrics::with_local_recorder(&recorder, || {
            let old = PeerBatchTxs::with_cap(PEER_BATCH_DEFER_TTL, 2);
            old.record_at(&[TxHash::from([1; 32])], t0);
            old.register_metrics(7);
            let clone = old.clone();
            old.register_metrics(7);
            assert!(matches!(occupancy("7"), Some(DebugValue::Gauge(value)) if value.0 == 1.0));

            let new = PeerBatchTxs::with_cap(PEER_BATCH_DEFER_TTL, 2);
            new.register_metrics(7);
            new.record_at(&[TxHash::from([2; 32]), TxHash::from([3; 32])], t0);
            let other_worker = PeerBatchTxs::with_cap(PEER_BATCH_DEFER_TTL, 2);
            other_worker.register_metrics(8);
            other_worker.record_at(&[TxHash::from([4; 32])], t0);
            assert!(matches!(occupancy("7"), Some(DebugValue::Gauge(value)) if value.0 == 3.0));
            assert!(matches!(occupancy("8"), Some(DebugValue::Gauge(value)) if value.0 == 1.0));

            drop(old);
            assert!(matches!(occupancy("7"), Some(DebugValue::Gauge(value)) if value.0 == 3.0));
            drop(clone);
            assert!(matches!(occupancy("7"), Some(DebugValue::Gauge(value)) if value.0 == 2.0));
            new.register_metrics(9);
            assert!(matches!(occupancy("7"), Some(DebugValue::Gauge(value)) if value.0 == 0.0));
            assert!(matches!(occupancy("9"), Some(DebugValue::Gauge(value)) if value.0 == 2.0));
        });
        ["7", "8", "9"].into_iter().for_each(|worker| {
            assert!(matches!(occupancy(worker), Some(DebugValue::Gauge(value)) if value.0 == 0.0));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a distinct hash from a small number.
    fn hash(n: u8) -> TxHash {
        TxHash::with_last_byte(n)
    }

    #[test]
    fn fresh_hash_is_deferred_and_unknown_hash_is_not() {
        let t0 = Instant::now();
        let window = PeerBatchTxs::new(PEER_BATCH_DEFER_TTL);
        window.record_at(&[hash(1)], t0);

        assert!(window.is_deferred_at(&hash(1), t0));
        assert!(!window.is_deferred_at(&hash(2), t0));
        assert_eq!(window.len(), 1);
        assert!(!window.is_empty());
    }

    #[test]
    fn deferral_expires_at_exactly_the_ttl() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let window = PeerBatchTxs::new(ttl);
        window.record_at(&[hash(1)], t0);

        // still deferred one millisecond before the TTL elapses
        assert!(window.is_deferred_at(&hash(1), t0 + ttl - Duration::from_millis(1)));
        // the TTL is exclusive: at exactly the TTL the transaction is buildable again
        assert!(!window.is_deferred_at(&hash(1), t0 + ttl));
    }

    #[test]
    fn re_record_within_the_ttl_does_not_refresh() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let window = PeerBatchTxs::new(ttl);
        window.record_at(&[hash(1)], t0);
        // a peer re-reports the same batch halfway through the deferral
        window.record_at(&[hash(1)], t0 + ttl / 2);

        // the deferral still ends one TTL after the FIRST report
        assert!(!window.is_deferred_at(&hash(1), t0 + ttl));
        assert_eq!(window.len(), 1);
    }

    #[test]
    fn re_record_after_expiry_is_immune_until_forgotten() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let window = PeerBatchTxs::new(ttl);
        window.record_at(&[hash(1)], t0);

        // expired but not yet forgotten: a fresh report cannot re-arm the deferral
        let re_report = t0 + ttl + Duration::from_millis(1);
        window.record_at(&[hash(1)], re_report);
        assert!(!window.is_deferred_at(&hash(1), re_report));
        assert!(!window.is_deferred_at(&hash(1), t0 + ttl * 2 - Duration::from_millis(1)));
    }

    #[test]
    fn re_record_after_twice_the_ttl_defers_again() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let window = PeerBatchTxs::new(ttl);
        window.record_at(&[hash(1)], t0);

        // the entry is forgotten at twice the TTL, so a later peer batch defers it once more
        let re_report = t0 + ttl * 2;
        window.record_at(&[hash(1)], re_report);
        assert!(window.is_deferred_at(&hash(1), re_report));
        assert_eq!(window.len(), 1);
    }

    #[test]
    fn full_window_drops_new_hashes_until_entries_age_out() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let window = PeerBatchTxs::with_cap(ttl, 2);
        // fill the window to its capacity
        window.record_at(&[hash(1), hash(2)], t0);

        // a hash recorded while the window is full is not remembered at all
        let later = t0 + Duration::from_secs(1);
        window.record_at(&[hash(3)], later);
        assert!(!window.is_deferred_at(&hash(3), later), "the new hash is dropped, not remembered");
        assert_eq!(window.len(), 2, "the window never exceeds its capacity");
        assert!(window.is_deferred_at(&hash(1), later), "no live entry is evicted");

        // once the filled entries age out at twice the TTL the same hash is recordable again
        let aged_out = t0 + ttl * 2;
        window.record_at(&[hash(3)], aged_out);
        assert!(window.is_deferred_at(&hash(3), aged_out));
        assert_eq!(window.len(), 1);
    }

    #[test]
    fn cap_holds_when_one_record_exceeds_it() {
        let t0 = Instant::now();
        let window = PeerBatchTxs::with_cap(PEER_BATCH_DEFER_TTL, 2);
        window.record_at(&[hash(1), hash(2), hash(3), hash(4)], t0);

        assert_eq!(window.len(), 2, "a single oversized batch is capped too");
        // the first `cap` hashes of the record are kept and the rest are dropped
        assert!(window.is_deferred_at(&hash(1), t0));
        assert!(window.is_deferred_at(&hash(2), t0));
        assert!(!window.is_deferred_at(&hash(3), t0));
        assert!(!window.is_deferred_at(&hash(4), t0));
    }

    /// A flood cannot evict a target's immune entry, so it cannot re-arm the target's deferral.
    #[test]
    fn full_window_cannot_re_arm_an_immune_entry() {
        let t0 = Instant::now();
        let ttl = PEER_BATCH_DEFER_TTL;
        let cap = 4;
        let window = PeerBatchTxs::with_cap(ttl, cap);
        // the target transaction is armed once, so its deferral ends at t0 + ttl
        window.record_at(&[hash(1)], t0);

        // a byzantine validator floods the window with `cap` junk hashes carried by structurally
        // valid batches, hoping to evict the target's expired-but-immune entry
        let flood = t0 + ttl;
        window.record_at(&[hash(2), hash(3), hash(4), hash(5)], flood);
        assert_eq!(window.len(), cap, "the flood fills the window but evicts nothing");
        assert!(!window.is_deferred_at(&hash(1), flood), "the target's deferral already ended");

        // the target is still remembered, so reporting it again cannot re-arm it
        let re_report = flood + Duration::from_secs(1);
        window.record_at(&[hash(1)], re_report);
        assert!(!window.is_deferred_at(&hash(1), re_report));
        assert!(!window.is_deferred_at(&hash(1), t0 + ttl * 2 - Duration::from_millis(1)));
    }
}
