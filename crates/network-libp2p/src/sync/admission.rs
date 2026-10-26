//! Committee-aware admission budgets for inbound sync streams.
//!
//! A responder admits an inbound sync stream against two kinds of budget, one per phase of the
//! exchange:
//!
//! 1. The opening-read budget: a [`ReservedPool`] whose permit is held only while the responder
//!    waits for the stream's opening request frame. A peer that opens a stream and then stays
//!    silent holds a permit from this budget until the opening-read timeout, and never holds a
//!    serve permit.
//! 2. The per-class serve pools: one [`ReservedPool`] per request class. A permit is taken once the
//!    opening request has been read and is held only while that request is served, so a flood of
//!    one request class cannot starve another.
//!
//! Each [`ReservedPool`] splits its permits into a public share, which any peer may take, and a
//! committee reserve, which only a [`PeerTier::Committee`] peer may take. A committee peer takes
//! the reserve first and falls back to the public share. Thus peers outside the committee that
//! fill the public share cannot block committee members from catching up. [`ReservedPeers`]
//! classifies a peer's BLS key into its [`PeerTier`].
//!
//! Across both phases and every class, [`PeerSlots`] caps the number of streams that one BLS key
//! has in flight. A [`PeerSlot`] is held for the whole life of a stream and frees its count when
//! it drops.
//!
//! No call in this module waits: a budget that is full refuses at once, so the caller can shed
//! the stream with a `Deny` frame instead of queueing it.

use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tn_types::BlsPublicKey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The admission tier of a peer, decided by its BLS key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerTier {
    /// A member of the current or the next committee. It may take permits from the committee
    /// reserve of a [`ReservedPool`] and from its public share.
    Committee,
    /// Any other peer. It may take permits only from the public share of a [`ReservedPool`].
    Public,
}

/// The set of BLS keys that admission classifies as [`PeerTier::Committee`].
///
/// The set is shared behind an [`Arc`], so a clone is cheap. The default set is empty and
/// classifies every peer as [`PeerTier::Public`].
#[derive(Clone, Debug, Default)]
pub struct ReservedPeers(Arc<BTreeSet<BlsPublicKey>>);

impl ReservedPeers {
    /// Build the reserved set from `keys`. A key that occurs more than once is kept once.
    pub fn new(keys: impl IntoIterator<Item = BlsPublicKey>) -> Self {
        Self(Arc::new(keys.into_iter().collect()))
    }

    /// Classify `peer`: [`PeerTier::Committee`] if its key is in the set, else
    /// [`PeerTier::Public`].
    pub fn tier(&self, peer: &BlsPublicKey) -> PeerTier {
        if self.0.contains(peer) {
            PeerTier::Committee
        } else {
            PeerTier::Public
        }
    }
}

/// A concurrency budget split into a public share and a committee reserve.
///
/// A [`PeerTier::Public`] peer takes permits only from the public share. A
/// [`PeerTier::Committee`] peer takes from the reserve first and then from the public share, so
/// the public share can be full while a committee peer is still admitted. A clone shares the same
/// permits.
#[derive(Clone, Debug)]
pub struct ReservedPool {
    /// The permits that any peer may take.
    public: Arc<Semaphore>,
    /// The permits that only a committee-tier peer may take.
    reserved: Arc<Semaphore>,
}

impl ReservedPool {
    /// Create a pool with `public` permits in the public share and `reserved` permits in the
    /// committee reserve.
    pub fn new(public: usize, reserved: usize) -> Self {
        Self {
            public: Arc::new(Semaphore::new(public)),
            reserved: Arc::new(Semaphore::new(reserved)),
        }
    }

    /// Try to take one permit for a peer of `tier`, without waiting.
    ///
    /// A committee-tier peer takes from the reserve first and falls back to the public share. A
    /// public-tier peer takes from the public share only. Returns `None` when no permit that the
    /// tier may take is free. The permit returns to the share it came from when it drops.
    pub fn try_acquire(&self, tier: PeerTier) -> Option<OwnedSemaphorePermit> {
        match tier {
            PeerTier::Committee => Self::take(&self.reserved).or_else(|| Self::take(&self.public)),
            PeerTier::Public => Self::take(&self.public),
        }
    }

    /// The number of free permits in the public share.
    pub fn available_public(&self) -> usize {
        self.public.available_permits()
    }

    /// The number of free permits in the committee reserve.
    pub fn available_reserved(&self) -> usize {
        self.reserved.available_permits()
    }

    /// Take one permit from `share` without waiting, or `None` when it is empty.
    fn take(share: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
        share.clone().try_acquire_owned().ok()
    }
}

/// A per-peer cap on the number of in-flight streams.
///
/// Each BLS key may hold at most `cap` [`PeerSlot`]s at the same time. A clone shares the same
/// counts.
#[derive(Clone, Debug)]
pub struct PeerSlots {
    /// The number of slots that each peer holds. A peer that holds no slot has no entry.
    counts: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// The largest number of slots that one peer may hold at the same time.
    cap: usize,
}

impl PeerSlots {
    /// Create an empty per-peer cap that admits at most `cap` slots for each peer.
    pub fn new(cap: usize) -> Self {
        Self { counts: Arc::default(), cap }
    }

    /// Try to take one slot for `peer`, without waiting.
    ///
    /// Returns `None` when `peer` already holds `cap` slots. The slot frees its count when it
    /// drops.
    pub fn try_take(&self, peer: BlsPublicKey) -> Option<PeerSlot> {
        let mut counts = self.counts.lock();
        let held = counts.get(&peer).copied().unwrap_or(0);
        (held < self.cap).then(|| {
            *counts.entry(peer).or_insert(0) += 1;
            PeerSlot { counts: self.counts.clone(), peer }
        })
    }

    /// The number of slots that `peer` holds now.
    pub fn in_flight(&self, peer: &BlsPublicKey) -> usize {
        self.counts.lock().get(peer).copied().unwrap_or(0)
    }
}

/// One slot that a peer holds in a [`PeerSlots`] cap.
///
/// When the slot drops, it decrements the peer's count and removes the entry at zero.
#[derive(Debug)]
pub struct PeerSlot {
    /// The shared per-peer counts of the [`PeerSlots`] that issued this slot.
    counts: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// The peer whose count this slot holds.
    peer: BlsPublicKey,
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut counts = self.counts.lock();
        let emptied = counts.get_mut(&self.peer).is_some_and(|count| {
            *count = count.saturating_sub(1);
            *count == 0
        });
        if emptied {
            counts.remove(&self.peer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::BlsKeypair;

    /// A deterministic BLS public key for `seed`.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// A public-tier peer never takes a reserved permit: when the public share is empty it is
    /// refused, although the whole committee reserve is free.
    #[test]
    fn public_tier_cannot_touch_the_reserve() {
        let pool = ReservedPool::new(2, 3);
        let held: Vec<Option<OwnedSemaphorePermit>> =
            (0..2).map(|_| pool.try_acquire(PeerTier::Public)).collect();
        assert!(held.iter().all(Option::is_some));
        assert!(pool.try_acquire(PeerTier::Public).is_none());
        assert_eq!((pool.available_public(), pool.available_reserved()), (0, 3));
        drop(held);
        assert_eq!((pool.available_public(), pool.available_reserved()), (2, 3));
    }

    /// A committee-tier peer empties the reserve first, then falls back to the public share, and
    /// is refused only when both are empty. Each permit returns to its own share on drop.
    #[test]
    fn committee_tier_takes_the_reserve_first_then_the_public_share() {
        let pool = ReservedPool::new(1, 2);
        let first = pool.try_acquire(PeerTier::Committee);
        assert!(first.is_some());
        assert_eq!((pool.available_public(), pool.available_reserved()), (1, 1));
        let second = pool.try_acquire(PeerTier::Committee);
        assert!(second.is_some());
        assert_eq!((pool.available_public(), pool.available_reserved()), (1, 0));
        let third = pool.try_acquire(PeerTier::Committee);
        assert!(third.is_some());
        assert_eq!((pool.available_public(), pool.available_reserved()), (0, 0));
        assert!(pool.try_acquire(PeerTier::Committee).is_none());
        drop(third);
        assert_eq!((pool.available_public(), pool.available_reserved()), (1, 0));
        drop((first, second));
        assert_eq!((pool.available_public(), pool.available_reserved()), (1, 2));
    }

    /// Three public keys, each within the per-peer cap of two, fill a public share of five
    /// (2 + 2 + 1). A fourth public key is refused, but a committee key is still admitted from
    /// the reserve. This is the exhaustion that GHSA-9h5w-9282-pc3g describes.
    #[test]
    fn full_public_share_still_admits_a_committee_key() {
        let committee = key(1);
        let reserved = ReservedPeers::new([committee]);
        let slots = PeerSlots::new(2);
        let pool = ReservedPool::new(5, 4);
        let opens: Vec<BlsPublicKey> = [(key(10), 2), (key(11), 2), (key(12), 1)]
            .into_iter()
            .flat_map(|(peer, streams)| std::iter::repeat_n(peer, streams))
            .collect();
        let held: Vec<(PeerSlot, OwnedSemaphorePermit)> = opens
            .iter()
            .filter_map(|peer| slots.try_take(*peer).zip(pool.try_acquire(reserved.tier(peer))))
            .collect();
        assert_eq!(held.len(), 5);
        assert_eq!(pool.available_public(), 0);

        let late = key(13);
        assert_eq!(reserved.tier(&late), PeerTier::Public);
        assert!(slots.try_take(late).is_some());
        assert!(pool.try_acquire(reserved.tier(&late)).is_none());

        assert_eq!(reserved.tier(&committee), PeerTier::Committee);
        let committee_slot = slots.try_take(committee);
        let committee_permit = pool.try_acquire(reserved.tier(&committee));
        assert!(committee_slot.is_some());
        assert!(committee_permit.is_some());
        assert_eq!(pool.available_reserved(), 3);
    }

    /// Peer slots cap at `cap` for each key, are independent across keys, and free on drop. An
    /// entry is removed when its count reaches zero.
    #[test]
    fn peer_slots_cap_per_key_and_free_on_drop() {
        let slots = PeerSlots::new(2);
        let (a, b) = (key(1), key(2));
        let held_a: Vec<PeerSlot> = (0..3).filter_map(|_| slots.try_take(a)).collect();
        assert_eq!(held_a.len(), 2);
        assert!(slots.try_take(a).is_none());
        assert_eq!(slots.in_flight(&a), 2);

        let held_b = slots.try_take(b);
        assert!(held_b.is_some());
        assert_eq!(slots.in_flight(&b), 1);

        drop(held_a);
        assert_eq!(slots.in_flight(&a), 0);
        assert!(slots.try_take(a).is_some());
        assert_eq!(slots.in_flight(&a), 0);

        drop(held_b);
        assert_eq!(slots.in_flight(&b), 0);
        assert!(slots.counts.lock().is_empty());
    }

    /// A listed key is committee tier, an unlisted key is public tier, and the empty default set
    /// classifies every key as public tier.
    #[test]
    fn reserved_peers_classify_listed_keys_as_committee() {
        let (current, next, outsider) = (key(1), key(2), key(3));
        let reserved = ReservedPeers::new([current, next, current]);
        assert_eq!(reserved.tier(&current), PeerTier::Committee);
        assert_eq!(reserved.tier(&next), PeerTier::Committee);
        assert_eq!(reserved.tier(&outsider), PeerTier::Public);
        assert_eq!(ReservedPeers::default().tier(&current), PeerTier::Public);
    }
}
