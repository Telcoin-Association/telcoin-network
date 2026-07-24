//! Unit tests for cache used for peers

use super::*;
use libp2p::PeerId;

#[test]
fn test_cache_entries_exist() {
    let mut cache = BannedPeerCache::new(Duration::from_secs(1), 100);
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    cache.insert(peer1);
    cache.insert(peer2);

    // assert peers already exist
    assert!(!cache.insert(peer2).0);
    assert!(!cache.insert(peer1).0);

    // assert removal
    assert!(cache.remove(&peer1));
    assert!(cache.remove(&peer2));

    // assert already removed
    assert!(!cache.remove(&peer1));
    assert!(!cache.remove(&peer2));
}

#[test]
fn test_remove_expired() {
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(100), 100, clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();

    // insert peers into the cache
    assert!(cache.insert(peer1).0, "Peer1 should be newly inserted");
    assert!(cache.insert(peer2).0, "Peer2 should be newly inserted");

    // advance the clock, but not enough to expire anything
    clock.advance(Duration::from_millis(25));

    // insert third peer after the delay
    assert!(cache.insert(peer3).0, "Peer3 should be newly inserted");

    // assert no peers expired yet
    let expired = cache.heartbeat();
    assert!(expired.is_empty(), "No peers should be expired yet");

    // explicitly remove peer2
    assert!(cache.remove(&peer2), "Peer2 should be successfully removed");

    // advance past peer1's 100ms expiry, but not peer3's (inserted 25ms later)
    clock.advance(Duration::from_millis(76));

    // peer1 was inserted 101ms ago -> expired
    // peer2 already removed
    // peer3 was inserted 76ms ago -> still valid
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Peer1 should be expired");

    // assert peer1 was removed
    assert!(expired.contains(&peer1), "Peer1 expired");
    // assert not expired
    assert!(!expired.contains(&peer2), "Peer2 already removed");
    assert!(!expired.contains(&peer3), "Peer3 is not expired yet");

    // advance until peer3 expires (now 126ms after its insertion)
    clock.advance(Duration::from_millis(25));

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
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(100), 100, clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    // insert peers into the cache
    cache.insert(peer1);
    cache.insert(peer2);

    // advance some time
    clock.advance(Duration::from_millis(60));

    // reinsert peer1 - this should reset the expiration timer
    assert!(!cache.insert(peer1).0, "Peer1 already inserted, but returned true");

    // advance another period that would expire peer2 but not the reinstated peer1
    clock.advance(Duration::from_millis(50));

    // assert only peer2 expired
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Only peer2 should be expired");
    assert!(expired.contains(&peer2), "Peer2 should be expired");
    assert!(!expired.contains(&peer1), "Peer1 should not be expired due to reinsertion");

    // advance until the reinserted peer1 expires
    clock.advance(Duration::from_millis(60));

    // assert peer1 expired
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Peer1 should now be expired");
    assert!(expired.contains(&peer1), "The expired peer should be peer1");
}

#[test]
fn test_remove_expired_empty_cache() {
    // Create a cache
    let mut cache: BannedPeerCache<String> = BannedPeerCache::new(Duration::from_millis(50), 100);

    // assert removing from an empty cache works correctly
    let expired = cache.heartbeat();
    assert!(expired.is_empty(), "No elements should be expired from an empty cache");
}

#[test]
fn test_remove_expired_ordering() {
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(500), 100, clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();
    let peer4 = PeerId::random();

    // insert with gaps between insertions
    cache.insert(peer1);
    clock.advance(Duration::from_millis(100));
    cache.insert(peer2);
    clock.advance(Duration::from_millis(100));
    cache.insert(peer3);
    clock.advance(Duration::from_millis(100));
    cache.insert(peer4);

    // advance until peer1 (inserted 510ms ago) passes its 500ms expiry
    clock.advance(Duration::from_millis(210));

    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Only Peer1 should be expired");
    assert_eq!(expired[0], peer1, "Peer1 should be expired");

    clock.advance(Duration::from_millis(100)); // advance until peer2 expires
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Only peer2 should be expired");
    assert_eq!(expired[0], peer2, "Peer2 should be expired");

    clock.advance(Duration::from_millis(200)); // advance until peer3 and peer4 expire
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 2, "Peer3 and Peer4 should be expired");
    assert_eq!(expired[0], peer3, "The first expired element should be peer3");
    assert_eq!(expired[1], peer4, "The second expired element should be peer4");
}

#[test]
fn test_size_cap_evicts_oldest() {
    // A cap of 3 with 5 distinct inserts must retain only the 3 newest, evicting the 2 oldest FIFO.
    // The duration is long enough that nothing ages out, so eviction is driven purely by the cap.
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_secs(600), 3, clock.clone());
    let peers: Vec<PeerId> = (0..5).map(|_| PeerId::random()).collect();

    // advance between inserts so insertion order is strict; the cap trims on every insert, so the
    // 4th and 5th inserts each surface the evicted oldest key.
    let evicted: Vec<PeerId> = peers
        .iter()
        .filter_map(|&peer| {
            let (_is_new, evicted) = cache.insert(peer);
            clock.advance(Duration::from_millis(1));
            evicted
        })
        .collect();

    // the cache reports the two oldest keys as evicted, in FIFO order, so the manager can unban
    // them and keep dependent state (the gossipsub blacklist) in sync.
    assert_eq!(evicted, vec![peers[0], peers[1]], "cap eviction returns the oldest keys FIFO");

    // occupancy is pinned at the cap, never the 5 inserted.
    assert_eq!(cache.len(), 3, "cache length must never exceed its size cap");

    // the two oldest keys were evicted...
    assert!(!cache.contains(&peers[0]), "the oldest key should be evicted");
    assert!(!cache.contains(&peers[1]), "the second-oldest key should be evicted");

    // ...and the three newest remain.
    assert!(cache.contains(&peers[2]), "the newest keys should be retained");
    assert!(cache.contains(&peers[3]), "the newest keys should be retained");
    assert!(cache.contains(&peers[4]), "the newest keys should be retained");
}

#[test]
fn test_size_cap_preserves_expiry() {
    // With headroom under the cap, age-based expiry must behave exactly as it does without a cap.
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(100), 10, clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    cache.insert(peer1);
    clock.advance(Duration::from_millis(50));
    cache.insert(peer2);

    // nothing has aged out yet, and the cap has not interfered.
    assert!(cache.heartbeat().is_empty(), "no peer should expire before its duration elapses");

    // advance past peer1's expiry but not peer2's.
    clock.advance(Duration::from_millis(60));
    let expired = cache.heartbeat();
    assert_eq!(expired, vec![peer1], "only the aged-out peer should be evicted by expiry");
    assert!(cache.contains(&peer2), "peer2 is still within its ban window");
}

#[test]
fn test_reinsert_at_capacity_evicts_nothing() {
    // Re-inserting an already-cached key must not trip the size cap: the list length is unchanged,
    // so no distinct key is dropped.
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_secs(600), 3, clock.clone());
    let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
    peers.iter().for_each(|&peer| {
        cache.insert(peer);
        clock.advance(Duration::from_millis(1));
    });
    assert_eq!(cache.len(), 3, "cache is full at the cap");

    // re-insert the oldest key; it moves to the back but nothing is evicted.
    let (is_new, evicted) = cache.insert(peers[0]);
    assert!(!is_new, "re-inserting an existing key returns false");
    assert!(evicted.is_none(), "re-insertion must not evict any key");
    assert_eq!(cache.len(), 3, "re-insertion must not change occupancy");
    assert!(cache.contains(&peers[0]), "the re-inserted key is still present");
    assert!(cache.contains(&peers[1]), "other keys are untouched");
    assert!(cache.contains(&peers[2]), "other keys are untouched");
}

#[test]
fn test_zero_cap_is_clamped_to_one() {
    // A zero cap is clamped to one so the cache is never permanently empty; the most recent key
    // survives and older ones are evicted.
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_secs(600), 0, clock.clone());
    let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
    peers.iter().for_each(|&peer| {
        cache.insert(peer);
        clock.advance(Duration::from_millis(1));
    });

    assert_eq!(cache.len(), 1, "a zero cap is clamped to a single retained entry");
    assert!(cache.contains(&peers[2]), "the most recently inserted key survives");
    assert!(!cache.contains(&peers[0]), "older keys are evicted under the clamped cap");
    assert!(!cache.contains(&peers[1]), "older keys are evicted under the clamped cap");
}
