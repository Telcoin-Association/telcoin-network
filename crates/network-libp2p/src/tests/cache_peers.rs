//! Unit tests for cache used for peers

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
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(100), clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();

    // insert peers into the cache
    assert!(cache.insert(peer1), "Peer1 should be newly inserted");
    assert!(cache.insert(peer2), "Peer2 should be newly inserted");

    // advance the clock, but not enough to expire anything
    clock.advance(Duration::from_millis(25));

    // insert third peer after the delay
    assert!(cache.insert(peer3), "Peer3 should be newly inserted");

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
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(100), clock.clone());
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    // insert peers into the cache
    cache.insert(peer1);
    cache.insert(peer2);

    // advance some time
    clock.advance(Duration::from_millis(60));

    // reinsert peer1 - this should reset the expiration timer
    assert!(!cache.insert(peer1), "Peer1 already inserted, but returned true");

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
    let mut cache: BannedPeerCache<String> = BannedPeerCache::new(Duration::from_millis(50));

    // assert removing from an empty cache works correctly
    let expired = cache.heartbeat();
    assert!(expired.is_empty(), "No elements should be expired from an empty cache");
}

#[test]
fn test_remove_expired_ordering() {
    let clock = ManualClock::new();
    let mut cache = BannedPeerCache::with_clock(Duration::from_millis(500), clock.clone());
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
