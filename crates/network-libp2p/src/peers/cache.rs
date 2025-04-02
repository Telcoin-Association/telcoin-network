//! Time-based LRU cache for managing temporarily banned peers.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;
use std::time::Instant;

/// The element representing a temporarily banend peer
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
    /// The method is called during the peer manager's heartbeat interval to limit constant polling for the cache.
    pub(super) fn heartbeat(&mut self) -> Vec<Key> {
        if self.list.is_empty() {
            return Vec::new();
        }

        let mut removed_elements = Vec::new();
        let now = Instant::now();
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

        // One last check to make sure there are no duplicates in the list
        assert_eq!(self.list.len(), self.map.len());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use libp2p::PeerId;

    #[test]
    fn test_cache_entries_exist() {
        let mut cache = BannedPeerCache::new(Duration::from_secs(1));
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        cache.insert(peer1);
        cache.insert(peer2);

        // assert peers already exist
        assert!(!cache.insert(peer2));
        assert!(!cache.insert(peer1));

        // assert removal
        assert!(cache.remove(&peer1));
        assert!(cache.remove(&peer2));

        // assert already removed
        assert!(!cache.remove(&peer1));
        assert!(!cache.remove(&peer2));
    }

    #[test]
    fn test_remove_expired() {
        let mut cache = BannedPeerCache::new(Duration::from_millis(100));
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();
        let peer3 = PeerId::random();

        // insert peers into the cache
        assert!(cache.insert(peer1), "Peer1 should be newly inserted");
        assert!(cache.insert(peer2), "Peer2 should be newly inserted");

        // wait for a short time, but not enough to expire
        std::thread::sleep(Duration::from_millis(25));

        // insert third peer after a delay
        assert!(cache.insert(peer3), "Peer3 should be newly inserted");

        // assert no peers expired yet
        let expired = cache.heartbeat();
        assert!(expired.is_empty(), "No peers should be expired yet");

        // explicitly remove peer2
        assert!(cache.remove(&peer2), "Peer2 should be successfully removed");

        // wait long enough for peer1 and peer2 to expire, but not peer3
        std::thread::sleep(Duration::from_millis(76));

        // peer1 should be expired (inserted ~101ms ago)
        // peer2 already removed
        // peer3 should still be valid (inserted ~76ms ago)
        let expired = cache.heartbeat();
        assert_eq!(expired.len(), 1, "Peer1 should be expired");

        // assert peer1 was removed
        assert!(expired.contains(&peer1), "Peer1 expired");
        // assert not expired
        assert!(!expired.contains(&peer2), "Peer2 already removed");
        assert!(!expired.contains(&peer3), "Peer3 is not expired yet");

        // wait for peer3 to expire
        std::thread::sleep(Duration::from_millis(25));

        // Now peer3 should be expired as well
        let expired = cache.heartbeat();
        assert_eq!(expired.len(), 1, "One peer should be expired");
        assert!(expired.contains(&peer3), "Peer3 should now be expired");

        // Cache should now be empty
        let expired = cache.heartbeat();
        assert!(expired.is_empty(), "No more peers should be expired");
    }

    #[test]
    fn test_remove_expired_with_reinsertions() {
        // create a cache with a short expiration time
        let mut cache = BannedPeerCache::new(Duration::from_millis(100));
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        // insert peers into the cache
        cache.insert(peer1);
        cache.insert(peer2);

        // wait some time
        std::thread::sleep(Duration::from_millis(60));

        // reinsert peer1 - this should reset the expiration timer
        assert!(!cache.insert(peer1), "Peer1 already inserted, but returned true");

        // wait another period that would expire peer2 but not the reinstated peer1
        std::thread::sleep(Duration::from_millis(50));

        // assert only peer2 expired
        let expired = cache.heartbeat();
        assert_eq!(expired.len(), 1, "Only peer2 should be expired");
        assert!(expired.contains(&peer2), "Peer2 should be expired");
        assert!(!expired.contains(&peer1), "Peer1 should not be expired due to reinsertion");

        // wait for peer1 to expire
        std::thread::sleep(Duration::from_millis(60));

        // assert peer1 expired
        let expired = cache.heartbeat();
        assert_eq!(expired.len(), 1, "Peer1 should now be expired");
        assert!(expired.contains(&peer1), "The expired peer should be peer1");
    }

    #[test]
    fn test_remove_expired_empty_cache() {
        // Create a cache
        let mut cache: BannedPeerCache<String> = BannedPeerCache::new(Duration::from_millis(50));

        // assert removing from an empty cache works correctly
        let expired = cache.heartbeat();
        assert!(expired.is_empty(), "No elements should be expired from an empty cache");
    }

    #[test]
    fn test_remove_expired_ordering() {
        let mut cache = BannedPeerCache::new(Duration::from_millis(100));
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();
        let peer3 = PeerId::random();
        let peer4 = PeerId::random();

        // Insert multiple elements with delays to test expiration ordering
        cache.insert(peer1);
        std::thread::sleep(Duration::from_millis(20));
        cache.insert(peer2);
        std::thread::sleep(Duration::from_millis(20));
        cache.insert(peer3);
        std::thread::sleep(Duration::from_millis(20));
        cache.insert(peer4);

        // wait for peer1 to expire
        std::thread::sleep(Duration::from_millis(41));

        // assert peer1 expired
        let expired = cache.heartbeat();
        println!("expired: {expired:?}");
        assert_eq!(expired.len(), 1, "Only Peer1 should be expired");
        assert_eq!(expired[0], peer1, "Peer1 should be expired");

        // wait for peer2 to expire
        std::thread::sleep(Duration::from_millis(20));
        let expired = cache.heartbeat();
        println!("expired: {expired:?}");
        assert_eq!(expired.len(), 1, "Only peer2 should be expired");
        assert_eq!(expired[0], peer2, "Peer2 should be expired");

        // wait for peer3 and peer4 to expire
        std::thread::sleep(Duration::from_millis(41));
        let expired = cache.heartbeat();
        println!("expired: {expired:?}");
        assert_eq!(expired.len(), 2, "Peer3 and Peer4 should be expired");
        assert_eq!(expired[0], peer3, "The first expired element should be peer3");
        assert_eq!(expired[1], peer4, "The second expired element should be peer4");
    }
}
