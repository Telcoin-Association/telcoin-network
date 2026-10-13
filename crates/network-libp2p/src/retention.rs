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

/// A process budget with separate reservations for required keys and connected sources.
///
/// Connection traffic cannot consume the required-key allowance. A record with several owners
/// occupies one store row; reservations count unique required keys and authenticated sources.
#[derive(Debug)]
pub(crate) struct RetentionBudget {
    /// Maximum reservations in each ownership class, shared by every swarm in the process.
    limit: usize,
    /// Reservations for own, pinned, and committee keys.
    required: AtomicUsize,
    /// Reservations for authenticated connected sources, one per transport identity.
    connections: AtomicUsize,
}

impl RetentionBudget {
    /// Create a budget allowing at most twice `limit` record rows across all its stores.
    pub(crate) fn new(limit: usize) -> Self {
        Self { limit, required: AtomicUsize::new(0), connections: AtomicUsize::new(0) }
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
/// the network removes it when the last transport connection closes.
#[derive(Debug)]
pub(crate) struct RecordRetention<Key: Eq + Hash, Source: Eq + Hash> {
    /// Shared required-key and connection reservation counters.
    budget: Arc<RetentionBudget>,
    /// Own and explicitly pinned keys, independent of committee rotation.
    pinned: HashSet<Key>,
    /// Union of pinned keys and the three authoritative committee sets.
    required: HashSet<Key>,
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
        self.required.contains(key) || self.connections.values().any(|connected| connected == key)
    }

    /// An upper bound on unique rows, including overlapping owners only once on the required side.
    pub(crate) fn max_records(&self) -> usize {
        self.required.len() + self.connections.len()
    }

    /// Enumerate required keys independently of connection-owned records.
    pub(crate) fn required_keys(&self) -> impl Iterator<Item = &Key> {
        self.required.iter()
    }
}

impl<Key: Eq + Hash, Source: Eq + Hash> Drop for RecordRetention<Key, Source> {
    /// Return every reservation when the swarm is dropped, including on constructor failure.
    fn drop(&mut self) {
        self.budget.required.fetch_sub(self.required.len(), Ordering::AcqRel);
        self.budget.connections.fetch_sub(self.connections.len(), Ordering::AcqRel);
    }
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
}
