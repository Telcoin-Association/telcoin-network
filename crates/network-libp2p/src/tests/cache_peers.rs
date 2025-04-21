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
    // Use a larger timeout to reduce flakiness
    let mut cache = BannedPeerCache::new(Duration::from_millis(500));
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();
    let peer4 = PeerId::random();

    // Insert with larger gaps between insertions
    cache.insert(peer1);
    std::thread::sleep(Duration::from_millis(100));
    cache.insert(peer2);
    std::thread::sleep(Duration::from_millis(100));
    cache.insert(peer3);
    std::thread::sleep(Duration::from_millis(100));
    cache.insert(peer4);

    // Wait with more buffer time
    std::thread::sleep(Duration::from_millis(210)); // Wait for peer1 to expire

    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Only Peer1 should be expired");
    assert_eq!(expired[0], peer1, "Peer1 should be expired");

    std::thread::sleep(Duration::from_millis(100)); // Wait for peer2 to expire
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 1, "Only peer2 should be expired");
    assert_eq!(expired[0], peer2, "Peer2 should be expired");

    std::thread::sleep(Duration::from_millis(200)); // Wait for peer3 and peer4 to expire
    let expired = cache.heartbeat();
    assert_eq!(expired.len(), 2, "Peer3 and Peer4 should be expired");
    assert_eq!(expired[0], peer3, "The first expired element should be peer3");
    assert_eq!(expired[1], peer4, "The second expired element should be peer4");
}
