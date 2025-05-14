//! Time-based LRU cache for managing temporarily banned peers.

use std::{
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

#[cfg(test)]
#[path = "../tests/cache_peers.rs"]
mod cache_peers;

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
pub(super) struct BannedPeerCache<Key> {
    /// The duplicate cache.
    map: HashSet<Key>,
    /// A list of keys sorted by the time they were inserted.
    list: VecDeque<Element<Key>>,
    /// The duration an element remains in the cache.
    duration: Duration,
}

impl<Key> BannedPeerCache<Key>
where
    Key: Eq + std::hash::Hash + Clone,
{
    /// Create a new instance of `Self`.
    pub(super) fn new(duration: Duration) -> Self {
        BannedPeerCache { map: HashSet::default(), list: VecDeque::new(), duration }
    }

    /// Insert a key and return true if the key does not already exist.
    ///
    /// NOTE: this does not remove expired elements
    pub(super) fn insert(&mut self, key: Key) -> bool {
        // insert into the map
        let is_new = self.map.insert(key.clone());

        // add the new key to the list, if it doesn't already exist.
        if is_new {
            self.list.push_back(Element { key, inserted: Instant::now() });
        } else {
            let position = self.list.iter().position(|e| e.key == key).expect("Key is not new");
            let mut element = self.list.remove(position).expect("Position is not occupied");
            element.inserted = Instant::now();
            self.list.push_back(element);
        }

        #[cfg(test)]
        self.check_invariant();

        is_new
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

        let now = Instant::now();
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
