//! Time-based LRU cache for managing temporarily banned peers.

use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

#[cfg(test)]
#[path = "../tests/cache_peers.rs"]
mod cache_peers;

/// Source of the current [`Instant`] used to timestamp insertions and evaluate expiry.
///
/// Production uses [`SystemClock`], which reads the real monotonic clock. Tests inject a manually
/// advanced clock so expiry is deterministic and does not depend on wall-clock sleeps.
pub(super) trait Clock {
    /// Return the current instant.
    fn now(&self) -> Instant;
}

/// The real monotonic clock backed by [`Instant::now`].
#[derive(Debug)]
pub(super) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A manually advanced [`Clock`] for deterministic expiry tests.
///
/// The instant only moves forward when [`ManualClock::advance`] is called, so tests control expiry
/// precisely without `std::thread::sleep`. The handle is cheaply cloneable and shares its
/// underlying instant, letting a test advance the same clock the cache reads.
#[cfg(test)]
#[derive(Debug, Clone)]
struct ManualClock {
    now: std::rc::Rc<std::cell::Cell<Instant>>,
}

#[cfg(test)]
impl ManualClock {
    /// Create a clock anchored at the current instant.
    fn new() -> Self {
        ManualClock { now: std::rc::Rc::new(std::cell::Cell::new(Instant::now())) }
    }

    /// Advance the clock by `duration`.
    fn advance(&self, duration: Duration) {
        self.now.set(self.now.get() + duration);
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.now.get()
    }
}

/// The element representing a temporarily banend peer
#[derive(Debug)]
struct Element<Key> {
    /// The key being inserted.
    key: Key,
    /// The instant the key was inserted.
    inserted: Instant,
}

/// This is a manual implementation of an LRU cache.
///
/// This implementation requires manually managing the cache.
/// The cache is intended to only be updated during the peer manager's heartbeat interval.
#[derive(Debug)]
pub(super) struct BannedPeerCache<Key, C = SystemClock> {
    /// The duplicate cache.
    map: HashSet<Key>,
    /// A list of keys sorted by the time they were inserted.
    list: VecDeque<Element<Key>>,
    /// The duration an element remains in the cache.
    duration: Duration,
    /// The maximum number of keys retained. On overflow the oldest key is evicted on insert, so
    /// occupancy never exceeds this bound regardless of the ban admission rate between heartbeats.
    max_len: usize,
    /// The clock used to timestamp insertions and evaluate expiry.
    clock: C,
}

impl<Key> BannedPeerCache<Key, SystemClock>
where
    Key: Eq + std::hash::Hash + Clone,
{
    /// Create a new instance of `Self` backed by the real system clock.
    ///
    /// `max_len` caps the number of retained keys; on overflow the oldest is evicted. It is clamped
    /// to at least one, because a zero cap would evict every freshly inserted key and leave the
    /// cache permanently empty, defeating the ban lookup that reads it.
    pub(super) fn new(duration: Duration, max_len: usize) -> Self {
        BannedPeerCache {
            map: HashSet::default(),
            list: VecDeque::new(),
            duration,
            max_len: max_len.max(1),
            clock: SystemClock,
        }
    }
}

impl<Key, C> BannedPeerCache<Key, C>
where
    Key: Eq + std::hash::Hash + Clone,
    C: Clock,
{
    /// Create a new instance of `Self` backed by the provided clock.
    ///
    /// `max_len` is clamped to at least one, matching [`BannedPeerCache::new`].
    #[cfg(test)]
    pub(super) fn with_clock(duration: Duration, max_len: usize, clock: C) -> Self {
        BannedPeerCache {
            map: HashSet::default(),
            list: VecDeque::new(),
            duration,
            max_len: max_len.max(1),
            clock,
        }
    }

    /// Insert a key, reporting whether it was new and any key evicted to honor the size cap.
    ///
    /// The first element is `true` when `key` was not already present. The second is the oldest key
    /// evicted to keep the cache within `max_len` (only ever `Some` on a new insert), so the caller
    /// can keep dependent state in sync with that removal, mirroring the keys returned by
    /// [`BannedPeerCache::heartbeat`] on age-based eviction.
    ///
    /// NOTE: this does not remove expired elements
    pub(super) fn insert(&mut self, key: Key) -> (bool, Option<Key>) {
        // insert into the map
        let is_new = self.map.insert(key.clone());

        // add the new key to the list, if it doesn't already exist.
        let evicted = if is_new {
            self.list.push_back(Element { key, inserted: self.clock.now() });

            // Enforce the size ceiling. Only a brand-new key grows the list, and the cap is
            // applied on every insert, so the list can exceed `max_len` by at most one here;
            // evicting the single oldest entry restores `len() <= max_len` without a loop.
            // `max_len >= 1` (clamped in the constructors) guarantees the evicted front is an
            // older entry, never the key just inserted.
            if self.list.len() > self.max_len {
                self.list.pop_front().map(|oldest| {
                    self.map.remove(&oldest.key);
                    oldest.key
                })
            } else {
                None
            }
        } else {
            let position = self.list.iter().position(|e| e.key == key).expect("Key is not new");
            let mut element = self.list.remove(position).expect("Position is not occupied");
            element.inserted = self.clock.now();
            self.list.push_back(element);
            None
        };

        #[cfg(test)]
        self.check_invariant();

        (is_new, evicted)
    }

    /// Remove a key from the cache and return true if the key existed.
    ///
    /// NOTE: this does not remove expired elements
    pub(super) fn remove(&mut self, key: &Key) -> bool {
        if self.map.remove(key) {
            let position = self.list.iter().position(|e| &e.key == key).expect("Key must exist");
            self.list.remove(position).expect("Position is not occupied");
            true
        } else {
            false
        }
    }

    /// Remove and return all expired elements from the cache.
    ///
    /// The method is called during the peer manager's heartbeat interval to limit constant polling
    /// for the cache.
    pub(super) fn heartbeat(&mut self) -> Vec<Key> {
        if self.list.is_empty() {
            return Vec::new();
        }

        let now = self.clock.now();
        let mut removed_elements = Vec::new();
        // remove any expired results
        while let Some(element) = self.list.pop_front() {
            if element.inserted + self.duration > now {
                self.list.push_front(element);
                break;
            }
            self.map.remove(&element.key);
            removed_elements.push(element.key);
        }

        #[cfg(test)]
        self.check_invariant();

        removed_elements
    }

    /// Check if the key is in the cache.
    pub(super) fn contains(&self, key: &Key) -> bool {
        self.map.contains(key)
    }

    /// Return the number of elements in the cache.
    pub(super) fn len(&self) -> usize {
        self.map.len()
    }

    #[cfg(test)]
    #[track_caller]
    fn check_invariant(&self) {
        // The list should be sorted. First element should have the oldest insertion
        let mut prev_insertion_time = None;
        for e in &self.list {
            match prev_insertion_time {
                Some(prev) => {
                    if prev <= e.inserted {
                        prev_insertion_time = Some(e.inserted);
                    } else {
                        panic!("List is not sorted by insertion time")
                    }
                }
                None => prev_insertion_time = Some(e.inserted),
            }
            // The key should be in the map
            assert!(self.map.contains(&e.key), "List and map should be in sync");
        }

        for k in &self.map {
            let _ =
                self.list.iter().position(|e| &e.key == k).expect("Map and list should be in sync");
        }

        // assert there are no duplicates in the list
        assert_eq!(self.list.len(), self.map.len());
    }
}
