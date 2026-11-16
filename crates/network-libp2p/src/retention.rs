//! Record ownership and shared process admission for Kademlia discovery records.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// A process budget with separate reservations for required keys, tracked keys, and connected
/// sources.
///
/// Connection traffic cannot consume the required-key allowance. Tracked keys share that allowance
/// but take only headroom no required key holds: required reservations never read the tracked
/// count, so tracked keys can never make a required reservation fail, while a tracked reservation
/// is cut to the headroom that required keys and the other owners' tracked keys leave. Every count
/// therefore stays within `limit`, and required plus tracked exceeds it only after required keys
/// grow into headroom tracked keys already hold, until those owners' next tracked updates. A
/// record with several owners occupies one store row; reservations count unique keys per class
/// and authenticated sources, and a tracked key its owner already requires reserves no tracked
/// slot (see [`RecordRetention::track`]).
#[derive(Debug)]
pub(crate) struct RetentionBudget {
    /// Maximum reservations in each ownership class, shared by every swarm in the process.
    limit: usize,
    /// Reservations for own, pinned, and committee keys.
    required: AtomicUsize,
    /// Reservations for tracked validator keys, bounded by the headroom `required` leaves.
    tracked: AtomicUsize,
    /// Reservations for authenticated connected sources, one per transport identity.
    connections: AtomicUsize,
}

impl RetentionBudget {
    /// Create a budget allowing at most `limit` reservations per class across all its stores.
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            required: AtomicUsize::new(0),
            tracked: AtomicUsize::new(0),
            connections: AtomicUsize::new(0),
        }
    }

    /// Atomically replace this owner's reservation count without consuming another owner's slots.
    fn reserve(&self, counter: &AtomicUsize, previous: usize, next: usize) -> Result<(), Capacity> {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(previous)
                    .and_then(|other| other.checked_add(next))
                    .filter(|total| *total <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| Capacity)
    }

    /// Atomically replace this owner's tracked reservation count with as much of `wanted` as the
    /// headroom allows, and return the count granted.
    ///
    /// The headroom is `limit` less every required reservation and every other owner's tracked
    /// reservation. The required count is read and never written here, so a grant never takes a
    /// slot a required key holds. A grant falls below `previous` when required keys have grown
    /// into headroom this owner held.
    fn reserve_tracked(&self, previous: usize, wanted: usize) -> usize {
        let mut granted = 0;
        // the closure never declines, so the grant of its final call is the one stored
        let _ = self.tracked.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            let others = used.saturating_sub(previous);
            let headroom = self.limit.saturating_sub(self.required.load(Ordering::Acquire));
            granted = wanted.min(headroom.saturating_sub(others));
            Some(others + granted)
        });
        granted
    }
}

/// The relevant process ownership class has exhausted its finite allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Capacity;

impl fmt::Display for Capacity {
    /// Describe the admission failure without treating it as a sender authentication failure.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Kademlia process retention budget exhausted")
    }
}

impl std::error::Error for Capacity {}

/// Independent record owners for one swarm.
///
/// Queries own only their in-flight result in the network task. They never pin a store row.
/// A connection owner exists only after its signed binding matches the authenticated source;
/// the network removes it when the last transport connection closes. Tracked keys are kept and
/// served like required keys but are never reported as required, so this policy never asks the
/// network to look them up; a tracked update keeps only as many keys as the headroom admits.
#[derive(Debug)]
pub(crate) struct RecordRetention<Key: Eq + Hash, Source: Eq + Hash> {
    /// Shared required-key, tracked-key, and connection reservation counters.
    budget: Arc<RetentionBudget>,
    /// Own and explicitly pinned keys, independent of committee rotation.
    pinned: HashSet<Key>,
    /// Union of pinned keys and the three authoritative committee sets.
    required: HashSet<Key>,
    /// Scheduled and eligible validator keys, replaced as a whole by each tracked update.
    tracked: HashSet<Key>,
    /// Tracked slots this owner holds in the budget: the tracked keys that were not required
    /// when the last tracked update reserved them.
    tracked_reserved: usize,
    /// The current verified record key belonging to each authenticated connected source.
    connections: HashMap<Source, Key>,
}

impl<Key: Eq + Hash + Clone, Source: Eq + Hash> RecordRetention<Key, Source> {
    /// Reserve the node's own record before admitting any remote records.
    pub(crate) fn new(own: Key, budget: Arc<RetentionBudget>) -> Result<Self, Capacity> {
        budget.reserve(&budget.required, 0, 1)?;
        Ok(Self {
            budget,
            pinned: HashSet::from([own.clone()]),
            required: HashSet::from([own]),
            tracked: HashSet::new(),
            tracked_reserved: 0,
            connections: HashMap::new(),
        })
    }

    /// Pin operator-provisioned keys atomically; a failed admission leaves all ownership unchanged.
    pub(crate) fn pin(&mut self, keys: impl IntoIterator<Item = Key>) -> Result<(), Capacity> {
        let additions: HashSet<_> = keys.into_iter().collect();
        let required: HashSet<_> = self.required.union(&additions).cloned().collect();
        self.budget.reserve(&self.budget.required, self.required.len(), required.len())?;
        self.pinned.extend(additions);
        self.required = required;
        Ok(())
    }

    /// Replace all committee ownership while preserving pins and connection ownership.
    pub(crate) fn committees(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
    ) -> Result<(), Capacity> {
        let required: HashSet<_> = self.pinned.iter().cloned().chain(keys).collect();
        self.budget.reserve(&self.budget.required, self.required.len(), required.len())?;
        self.required = required;
        Ok(())
    }

    /// Replace all tracked ownership with as many of `keys` as the tracked headroom admits,
    /// preserving pins, committees, and connection ownership.
    ///
    /// Keys are kept in the order given and a repeated key counts once, so the caller lists them
    /// by priority. The keys past the headroom are left out instead of refusing the update, so a
    /// tracked update never fails.
    ///
    /// A key this owner already requires holds a required slot for the same row, so it is tracked
    /// without a tracked slot and is never left out; only the other keys are reserved, and cut to
    /// the headroom, in the order given. If such a key then leaves the committees, its row stays
    /// retained through tracking without a slot until this owner's next tracked update reserves
    /// it or leaves it out. Until then this owner's rows exceed its reservations by at most the
    /// number of required keys the last update tracked.
    pub(crate) fn track(&mut self, keys: impl IntoIterator<Item = Key>) -> TrackedUpdate<Key> {
        let mut seen = HashSet::new();
        let (required, outside): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .filter(|key| seen.insert(key.clone()))
            .partition(|key| self.required.contains(key));
        let granted = self.budget.reserve_tracked(self.tracked_reserved, outside.len());
        self.tracked_reserved = granted;
        let mut outside = outside.into_iter();
        let tracked: HashSet<_> =
            required.into_iter().chain(outside.by_ref().take(granted)).collect();
        let left_out = outside.collect();
        let previous = std::mem::replace(&mut self.tracked, tracked);
        let released = previous.into_iter().filter(|key| !self.tracked.contains(key)).collect();
        TrackedUpdate { released, left_out }
    }

    /// Retain one current binding per authenticated source; a rekey relinquishes its previous key.
    pub(crate) fn connected(&mut self, source: Source, key: Key) -> Result<Option<Key>, Capacity> {
        if !self.connections.contains_key(&source) {
            self.budget.reserve(&self.budget.connections, 0, 1)?;
        }
        Ok(self.connections.insert(source, key))
    }

    /// Relinquish only this source's connection ownership after its last connection closes.
    pub(crate) fn disconnected(&mut self, source: &Source) -> Option<Key> {
        self.connections.remove(source).inspect(|_| {
            self.budget.connections.fetch_sub(1, Ordering::AcqRel);
        })
    }

    /// Whether any independent owner still requires a row for this key.
    pub(crate) fn retains(&self, key: &Key) -> bool {
        self.required.contains(key)
            || self.tracked.contains(key)
            || self.connections.values().any(|connected| connected == key)
    }

    /// An upper bound on unique rows: overlaps count once within a class, but a key held by
    /// several classes counts in each.
    pub(crate) fn max_records(&self) -> usize {
        self.required.len() + self.tracked.len() + self.connections.len()
    }

    /// Enumerate required keys independently of tracked and connection-owned records.
    pub(crate) fn required_keys(&self) -> impl Iterator<Item = &Key> {
        self.required.iter()
    }
}

impl<Key: Eq + Hash, Source: Eq + Hash> Drop for RecordRetention<Key, Source> {
    /// Return every reservation when the swarm is dropped, including on constructor failure.
    fn drop(&mut self) {
        self.budget.required.fetch_sub(self.required.len(), Ordering::AcqRel);
        self.budget.tracked.fetch_sub(self.tracked_reserved, Ordering::AcqRel);
        self.budget.connections.fetch_sub(self.connections.len(), Ordering::AcqRel);
    }
}

/// What one tracked update changed, as [`RecordRetention::track`] reports it.
#[derive(Debug)]
pub(crate) struct TrackedUpdate<Key> {
    /// Keys the update stopped tracking, so the caller can prune the rows among them that lost
    /// their final owner.
    pub(crate) released: Vec<Key>,
    /// Keys past the tracked headroom, in the order given, which the update does not track.
    pub(crate) left_out: Vec<Key>,
}

#[cfg(test)]
mod tests {
    //! Ownership overlap, rotation, capacity isolation, and shared reservation conservation.

    use super::*;

    /// Pins, committees, and another source survive an overlapping connection owner's release.
    #[test]
    fn overlapping_owners_survive_disconnect() -> Result<(), Capacity> {
        let mut policy = RecordRetention::new(0, Arc::new(RetentionBudget::new(8)))?;
        policy.pin([1])?;
        policy.committees([1, 2, 2, 3])?;
        assert_eq!(policy.max_records(), 4, "committee and pin overlaps count one required key");
        assert_eq!(policy.required_keys().count(), 4);
        policy.connected(10, 1)?;
        policy.connected(11, 2)?;
        policy.connected(12, 4)?;
        policy.connected(13, 4)?;
        policy.disconnected(&10);
        policy.disconnected(&11);
        policy.disconnected(&12);
        assert!([0, 1, 2, 3, 4].iter().all(|key| policy.retains(key)));
        policy.disconnected(&13);
        assert!(!policy.retains(&4));
        Ok(())
    }

    /// Rotation and rekeying remove only the keys that have lost their final owner.
    #[test]
    fn rotation_and_rekey_release_old_records() -> Result<(), Capacity> {
        let mut policy = RecordRetention::new(0, Arc::new(RetentionBudget::new(8)))?;
        policy.pin([1])?;
        policy.committees([1, 2, 3])?;
        policy.connected(10, 2)?;
        policy.committees([4])?;
        assert!(policy.retains(&1));
        assert!(policy.retains(&2));
        assert!(!policy.retains(&3));
        policy.connected(10, 5)?;
        assert!(!policy.retains(&2));
        assert!(policy.retains(&5));
        policy.disconnected(&10);
        assert!(!policy.retains(&5));
        assert!(policy.retains(&4));
        assert!(!policy.retains(&99));
        Ok(())
    }

    /// Full connection admission cannot consume the independent required-key reservation budget.
    #[test]
    fn connection_capacity_cannot_block_required_keys() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(2));
        let mut policy = RecordRetention::new(0, budget.clone())?;
        policy.connected(10, 10)?;
        policy.connected(11, 11)?;
        assert_eq!(policy.connected(12, 12), Err(Capacity));
        policy.pin([1])?;
        assert!(policy.retains(&1));
        assert!(!policy.retains(&12));
        assert_eq!(policy.pin([2]), Err(Capacity));
        assert_eq!(policy.committees([3]), Err(Capacity));
        assert!(policy.retains(&1));
        assert!(!policy.retains(&2));
        assert_eq!(budget.required.load(Ordering::Acquire), 2);
        assert_eq!(budget.connections.load(Ordering::Acquire), 2);
        Ok(())
    }

    /// Different swarms share a finite ceiling and dropping a swarm returns both kinds of slots.
    #[test]
    fn process_budget_is_shared_and_released() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(2));
        let mut first = RecordRetention::new(0, budget.clone())?;
        let mut second = RecordRetention::new(1, budget.clone())?;
        assert!(RecordRetention::<usize, usize>::new(2, budget.clone()).is_err());
        first.connected(10, 10)?;
        second.connected(11, 11)?;
        assert_eq!(first.connected(12, 12), Err(Capacity));
        drop(second);
        first.pin([2])?;
        first.connected(12, 12)?;
        drop(first);
        assert_eq!(budget.required.load(Ordering::Acquire), 0);
        assert_eq!(budget.connections.load(Ordering::Acquire), 0);
        Ok(())
    }

    /// Keys in a fixed order, since ownership sets do not order what they return.
    fn sorted<Key: Ord>(mut keys: Vec<Key>) -> Vec<Key> {
        keys.sort_unstable();
        keys
    }

    /// Tracked keys use only the headroom required keys leave: once they hold all of it, a larger
    /// tracked update is cut to it while a required reservation still succeeds, and the next
    /// tracked update gives up the headroom required keys grew into.
    #[test]
    fn tracked_capacity_cannot_block_required_keys() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(4));
        let mut policy = RecordRetention::<usize, usize>::new(0, budget.clone())?;
        assert!(policy.track([10, 11, 12]).left_out.is_empty());
        assert_eq!(policy.track([10, 11, 12, 13]).left_out, [13], "tracked keys hold the headroom");
        assert!(!policy.retains(&13));
        policy.committees([1, 2, 3])?;
        assert!([1, 2, 3, 10, 11, 12].iter().all(|key| policy.retains(key)));
        assert_eq!(budget.required.load(Ordering::Acquire), 4, "required keys fill the allowance");
        assert_eq!(budget.tracked.load(Ordering::Acquire), 3);
        // required keys now hold the headroom, so even a smaller tracked update keeps nothing
        let update = policy.track([10, 11]);
        assert_eq!(update.left_out, [10, 11]);
        assert_eq!(sorted(update.released), [10, 11, 12]);
        assert!(!policy.retains(&10));
        assert_eq!(budget.tracked.load(Ordering::Acquire), 0);
        Ok(())
    }

    /// Required keys that fill the allowance leave no headroom, so a tracked update keeps nothing
    /// and changes no other class; a smaller committee makes room again.
    #[test]
    fn required_capacity_blocks_tracked_keys() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(3));
        let mut policy = RecordRetention::<usize, usize>::new(0, budget.clone())?;
        policy.committees([1, 2])?;
        assert_eq!(policy.track([10]).left_out, [10]);
        assert!(!policy.retains(&10));
        assert_eq!(budget.tracked.load(Ordering::Acquire), 0);
        assert_eq!(budget.required.load(Ordering::Acquire), 3);
        policy.committees([1])?;
        assert!(policy.track([10]).left_out.is_empty());
        assert!(policy.retains(&10));
        Ok(())
    }

    /// Every swarm in the process shares the tracked headroom: one swarm's tracked keys can crowd
    /// out another swarm's tracked update but never its required keys, and dropping a swarm
    /// returns its tracked reservation.
    #[test]
    fn tracked_headroom_is_shared_and_released_on_drop() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(4));
        let mut first = RecordRetention::<usize, usize>::new(0, budget.clone())?;
        let mut second = RecordRetention::<usize, usize>::new(1, budget.clone())?;
        assert!(first.track([10, 11]).left_out.is_empty());
        assert_eq!(second.track([12]).left_out, [12], "the first swarm holds the headroom");
        second.committees([2, 3])?;
        assert_eq!(budget.required.load(Ordering::Acquire), 4);
        drop(first);
        assert_eq!(budget.tracked.load(Ordering::Acquire), 0, "drop returns the tracked slots");
        assert_eq!(budget.required.load(Ordering::Acquire), 3);
        assert!(second.track([12]).left_out.is_empty());
        drop(second);
        assert_eq!(budget.required.load(Ordering::Acquire), 0);
        assert_eq!(budget.tracked.load(Ordering::Acquire), 0);
        Ok(())
    }

    /// A tracked update keeps exactly the headroom, which is the limit less every required key
    /// and the other swarms' tracked keys, filled in the order given with a repeat counted once.
    #[test]
    fn tracked_update_keeps_the_exact_headroom_in_order() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(8));
        let mut policy = RecordRetention::<usize, usize>::new(0, budget.clone())?;
        let mut other = RecordRetention::<usize, usize>::new(1, budget.clone())?;
        policy.committees([2, 3])?;
        assert!(other.track([30]).left_out.is_empty());
        // eight slots less four required keys and the other swarm's tracked key leave three
        let update = policy.track([15, 11, 15, 14, 12, 13]);
        assert_eq!(update.left_out, [12, 13], "the keys past the headroom, in the order given");
        assert!(update.released.is_empty());
        assert!([15, 11, 14].iter().all(|key| policy.retains(key)));
        assert!(!policy.retains(&12) && !policy.retains(&13));
        let used = budget.required.load(Ordering::Acquire) + budget.tracked.load(Ordering::Acquire);
        assert_eq!(used, 8, "the update fills the allowance");
        assert!(
            policy.track([14, 11, 15]).left_out.is_empty(),
            "a set of exactly the headroom fits"
        );
        // the other swarm's own reservation stays its headroom, so a new order keeps its first key
        assert_eq!(other.track([31, 30]).left_out, [30]);
        assert!(other.retains(&31) && !other.retains(&30));
        assert_eq!(budget.tracked.load(Ordering::Acquire), 4);
        Ok(())
    }

    /// A tracked key the owner already requires takes no tracked slot and is never left out, so
    /// it cannot push a key outside the committees past the headroom. A key that then leaves the
    /// committees stays retained without a slot until the next tracked update reserves it.
    #[test]
    fn tracked_keys_already_required_take_no_tracked_slot() -> Result<(), Capacity> {
        let budget = Arc::new(RetentionBudget::new(8));
        let mut policy = RecordRetention::<usize, usize>::new(0, budget.clone())?;
        policy.committees([1, 2])?;
        // five slots of headroom hold the four keys outside the committees
        assert!(policy.track([1, 2, 3, 4, 5, 6]).left_out.is_empty());
        assert!([1, 2, 3, 4, 5, 6].iter().all(|key| policy.retains(key)));
        assert_eq!(budget.tracked.load(Ordering::Acquire), 4, "only keys outside required reserve");
        policy.committees([1])?;
        assert!(policy.retains(&2), "a key that left the committees stays tracked");
        assert_eq!(budget.required.load(Ordering::Acquire), 2);
        assert_eq!(
            budget.tracked.load(Ordering::Acquire),
            4,
            "without a slot until the next update"
        );
        // the headroom cuts only the keys outside required, so the required key listed last stays
        let update = policy.track([2, 3, 4, 5, 6, 7, 8, 1]);
        assert_eq!(update.left_out, [8]);
        assert!(update.released.is_empty());
        assert!([1, 2, 3, 4, 5, 6, 7].iter().all(|key| policy.retains(key)));
        assert_eq!(
            budget.tracked.load(Ordering::Acquire),
            6,
            "the headroom two required keys leave"
        );
        drop(policy);
        assert_eq!(budget.tracked.load(Ordering::Acquire), 0, "drop returns the reserved slots");
        assert_eq!(budget.required.load(Ordering::Acquire), 0);
        Ok(())
    }

    /// A tracked key is retained until an update stops tracking it, and that update returns it
    /// for pruning; a key another owner holds stays retained, and an update larger than the
    /// headroom keeps its first keys.
    #[test]
    fn tracked_keys_are_retained_until_released() -> Result<(), Capacity> {
        let mut policy = RecordRetention::new(0, Arc::new(RetentionBudget::new(6)))?;
        policy.committees([1])?;
        policy.connected(20, 2)?;
        assert!(policy.track([1, 2, 3, 4]).left_out.is_empty());
        assert!([1, 2, 3, 4].iter().all(|key| policy.retains(key)));
        assert_eq!(sorted(policy.track([4, 5]).released), [1, 2, 3]);
        assert!(policy.retains(&1), "a committee key outlives its tracked ownership");
        assert!(policy.retains(&2), "a connected key outlives its tracked ownership");
        assert!(!policy.retains(&3));
        let update = policy.track([4, 5, 6, 7, 8]);
        assert_eq!(update.left_out, [8], "the first keys that fit are kept");
        assert!(update.released.is_empty());
        assert!([4, 5, 6, 7].iter().all(|key| policy.retains(key)));
        assert!(!policy.retains(&8));
        Ok(())
    }

    /// The row bound counts every tracked key, including one a committee also owns, while the
    /// required keys, which drive the missing-record lookups, never include a tracked key.
    #[test]
    fn max_records_counts_tracked_keys_that_are_never_required() -> Result<(), Capacity> {
        let mut policy = RecordRetention::new(0, Arc::new(RetentionBudget::new(8)))?;
        policy.committees([1, 2])?;
        policy.connected(20, 9)?;
        assert_eq!(policy.max_records(), 4);
        assert!(policy.track([2, 3, 3, 4]).left_out.is_empty());
        assert_eq!(policy.max_records(), 7, "three tracked keys, one also a committee key");
        assert_eq!(sorted(policy.required_keys().copied().collect()), [0, 1, 2]);
        assert_eq!(sorted(policy.track(std::iter::empty()).released), [2, 3, 4]);
        assert_eq!(policy.max_records(), 4);
        Ok(())
    }
}
