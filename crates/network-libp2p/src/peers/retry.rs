//! Bounded retry memory and outage log budgets, with an explicit clock for deterministic tests.

use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

/// Longest retry delay, including prolonged partitions.
const MAX_DELAY: Duration = Duration::from_secs(120);
/// Idle failure entries expire without requiring a successful connection.
const RETENTION: Duration = Duration::from_secs(900);
/// Per-peer interval for repeated failure summaries.
const LOG_WINDOW: Duration = Duration::from_secs(60);
/// Maximum distinct actionable failure classes logged in one interval.
const LOG_CLASSES: usize = 8;

/// One endpoint's consecutive failures.
#[derive(Debug)]
struct Failure {
    /// Next permitted attempt.
    retry_at: Instant,
    /// Last real failure, independent of repeated advertisements or denied retries.
    failed_at: Instant,
    /// Delay after this failure.
    delay: Duration,
}

/// Failure memory survives discovery eviction and is bounded independently of peer churn.
#[derive(Debug)]
pub(super) struct DialBackoff<K> {
    /// Transport identities, reset only by authenticated recovery or accepted mapping changes.
    failures: HashMap<K, Failure>,
    /// Maximum retained identities, derived from the configured disconnected-peer budget.
    capacity: usize,
    /// Untracked failures share a finite deadline instead of permanently denying new identities.
    overflow_retry_at: Option<Instant>,
}

impl<K: Eq + Hash> DialBackoff<K> {
    /// Create a finite cache, retaining at least one outage entry.
    pub(super) fn new(capacity: usize) -> Self {
        Self { failures: HashMap::new(), capacity: capacity.max(1), overflow_retry_at: None }
    }

    /// Return the remaining delay. At capacity, new identities wait rather than evict live memory.
    pub(super) fn retry_after(&self, key: &K, now: Instant) -> Option<Duration> {
        self.failures.get(key).map_or_else(
            || {
                (self.failures.len() >= self.capacity)
                    .then_some(self.overflow_retry_at)
                    .flatten()
                    .filter(|deadline| now < *deadline)
                    .map(|deadline| deadline.duration_since(now))
            },
            |failure| (now < failure.retry_at).then(|| failure.retry_at.duration_since(now)),
        )
    }

    /// Record a real failure; rejected cooldown attempts never call this method.
    pub(super) fn failed(&mut self, key: K, now: Instant) {
        self.prune(now);
        let had_capacity = self.failures.len() < self.capacity;
        if self.failures.contains_key(&key) || self.failures.len() < self.capacity {
            let delay = self
                .failures
                .get(&key)
                .map_or(Duration::from_secs(1), |old| old.delay.saturating_mul(2).min(MAX_DELAY));
            self.failures.insert(key, Failure { retry_at: now + delay, failed_at: now, delay });
            if had_capacity && self.failures.len() == self.capacity {
                self.overflow_retry_at = Some(now + MAX_DELAY);
            }
        } else {
            self.overflow_retry_at = Some(now + MAX_DELAY);
        }
    }

    /// Forget stale failure memory after an accepted identity/endpoint change or connection.
    pub(super) fn reset(&mut self, key: &K) {
        self.failures.remove(key);
    }

    /// Reserve one retry slot for an accepted authoritative mapping, keeping the same hard cap.
    /// Unsigned hints and ordinary retry requests must not call this recovery path.
    pub(super) fn recover_mapping(&mut self, key: &K, now: Instant)
    where
        K: Clone,
    {
        self.prune(now);
        self.reset(key);
        let oldest = (self.failures.len() >= self.capacity).then_some(()).and_then(|()| {
            self.failures
                .iter()
                .min_by_key(|(_, failure)| failure.failed_at)
                .map(|(id, _)| id.clone())
        });
        oldest.into_iter().for_each(|id| {
            self.failures.remove(&id);
        });
    }

    /// Expire idle identities without letting advertisements extend their lifetime.
    pub(super) fn prune(&mut self, now: Instant) {
        self.failures.retain(|_, failure| now.duration_since(failure.failed_at) < RETENTION);
    }
}

/// Per-identity outage logging: first failures, distinct classes, periodic summaries and recovery.
#[derive(Debug)]
struct DialLogBudget<K> {
    /// Start of the current logging interval.
    window: Instant,
    /// Distinct classes already reported this interval.
    classes: HashMap<K, ()>,
    /// Failures accumulated since the last emitted diagnostic.
    suppressed: usize,
    /// Total failures for the eventual recovery summary.
    failures: usize,
}

/// Bounded outage memory owned by a swarm, shared by all of its dial tasks.
#[derive(Debug)]
pub(crate) struct DialLogBook<K> {
    /// Maximum tracked committee identities.
    capacity: usize,
    /// Outage budgets retained across task and epoch changes.
    identities: HashMap<K, DialLogBudget<String>>,
    /// A shared budget prevents overflow identities from bypassing the warning limit.
    overflow: DialLogBudget<String>,
}

impl<K: Eq + Hash> DialLogBook<K> {
    /// Create bounded per-swarm logging memory.
    pub(crate) fn new(capacity: usize, now: Instant) -> Self {
        Self {
            capacity: capacity.max(1),
            identities: HashMap::new(),
            overflow: DialLogBudget::new(now),
        }
    }

    /// Emit a diagnostic only when the identity's shared budget permits it.
    pub(crate) fn report(&mut self, identity: K, class: String, now: Instant) -> Option<usize> {
        if self.identities.contains_key(&identity) || self.identities.len() < self.capacity {
            self.identities
                .entry(identity)
                .or_insert_with(|| DialLogBudget::new(now))
                .report(class, now)
        } else {
            self.overflow.report(class, now)
        }
    }

    /// Consume a tracked outage once, so concurrent tasks cannot repeat recovery summaries.
    pub(crate) fn recovered(&mut self, identity: &K) -> Option<usize> {
        self.identities
            .remove(identity)
            .map(|budget| budget.failures())
            .filter(|failures| *failures > 0)
    }
}

impl<K: Eq + Hash> DialLogBudget<K> {
    /// Start a fresh outage budget for one committee identity and swarm.
    fn new(now: Instant) -> Self {
        Self { window: now, classes: HashMap::new(), suppressed: 0, failures: 0 }
    }

    /// Return a suppressed-failure count when a diagnostic should be emitted.
    fn report(&mut self, class: K, now: Instant) -> Option<usize> {
        self.failures = self.failures.saturating_add(1);
        if now.duration_since(self.window) >= LOG_WINDOW {
            self.window = now;
            self.classes.clear();
        }
        if !self.classes.contains_key(&class) && self.classes.len() < LOG_CLASSES {
            self.classes.insert(class, ());
            let suppressed = self.suppressed;
            self.suppressed = 0;
            Some(suppressed)
        } else {
            self.suppressed = self.suppressed.saturating_add(1);
            None
        }
    }

    /// Return the outage's total failure count for a single recovery summary.
    fn failures(&self) -> usize {
        self.failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_backoff_delay_and_recovery() {
        let start = Instant::now();
        let mut cache = DialBackoff::new(2);
        cache.failed(1, start);
        assert_eq!(cache.retry_after(&1, start), Some(Duration::from_secs(1)));
        assert_eq!(cache.retry_after(&1, start + Duration::from_secs(1)), None);
        let last = (1..12).fold(start, |at, _| {
            let next = at + cache.failures.get(&1).map_or(Duration::ZERO, |failure| failure.delay);
            cache.failed(1, next);
            next
        });
        assert_eq!(cache.retry_after(&1, last), Some(MAX_DELAY));
        assert_eq!(cache.retry_after(&1, last + MAX_DELAY), None);
        cache.reset(&1);
        assert_eq!(cache.retry_after(&1, last), None);
        cache.failed(1, last);
        assert_eq!(cache.retry_after(&1, last), Some(Duration::from_secs(1)));
    }

    #[test]
    fn exchange_backoff_capacity_and_idle_expiry() {
        let start = Instant::now();
        let mut cache = DialBackoff::new(2);
        (0..100).for_each(|key| cache.failed(key, start));
        assert_eq!(cache.failures.len(), 2);
        assert_eq!(cache.retry_after(&99, start), Some(MAX_DELAY));
        cache.recover_mapping(&99, start);
        assert_eq!(cache.failures.len(), 1);
        assert_eq!(cache.retry_after(&99, start), None);
        cache.failed(99, start);
        assert_eq!(cache.failures.len(), 2);
        cache.prune(start + RETENTION);
        assert!(cache.failures.is_empty());
        assert_eq!(cache.retry_after(&99, start + RETENTION), None);
    }

    #[test]
    fn exchange_backoff_saturation_has_a_finite_retry_deadline() {
        let start = Instant::now();
        let mut cache = DialBackoff::new(1);
        cache.failed(1, start);
        assert_eq!(cache.retry_after(&2, start), Some(MAX_DELAY));
        (1..120).for_each(|seconds| cache.failed(1, start + Duration::from_secs(seconds)));
        let expired = start + MAX_DELAY;
        assert_eq!(cache.retry_after(&2, expired), None);
        assert_eq!(cache.failures.len(), 1);
        cache.failed(2, expired);
        assert_eq!(cache.failures.len(), 1);
        assert_eq!(cache.retry_after(&3, expired), Some(MAX_DELAY));
        assert_eq!(cache.retry_after(&2, expired + MAX_DELAY), None);
        assert_eq!(cache.retry_after(&3, expired + MAX_DELAY), None);
    }

    #[test]
    fn exchange_outage_logs_first_distinct_periodic_and_recovery() {
        let start = Instant::now();
        let mut budget = DialLogBudget::new(start);
        assert_eq!(budget.report(1, start), Some(0));
        assert_eq!(budget.report(1, start), None);
        assert_eq!(budget.report(2, start), Some(1));
        assert_eq!((0..100).filter_map(|key| budget.report(key, start)).count(), 6);
        assert_eq!(budget.classes.len(), LOG_CLASSES);
        assert_eq!(budget.report(1, start + LOG_WINDOW), Some(92));
        assert_eq!(budget.failures(), 104);
    }

    #[test]
    fn exchange_outage_memory_and_overflow_are_bounded() {
        let start = Instant::now();
        let mut logs = DialLogBook::new(1, start);
        assert_eq!(logs.report(1, "transport".into(), start), Some(0));
        assert_eq!(logs.report(1, "transport".into(), start), None);
        assert_eq!(logs.report(2, "transport".into(), start), Some(0));
        assert_eq!((3..100).filter_map(|id| logs.report(id, "transport".into(), start)).count(), 0);
        assert_eq!(logs.identities.len(), 1);
        assert_eq!(logs.recovered(&1), Some(2));
        assert_eq!(logs.recovered(&1), None);
        assert_eq!(logs.report(2, "transport".into(), start + LOG_WINDOW), Some(0));
        assert_eq!(logs.recovered(&2), Some(1));
    }
}
