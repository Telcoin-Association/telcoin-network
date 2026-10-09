//! Reconcile gossip privileges from current policy and verified network identities.
//!
//! All three committee slots receive a pin. The previous slot is the entire committee
//! grace window; leaving that slot ends the pin unless another reason survives. An
//! unidentified connection, including one awaiting a record during admission grace,
//! receives no pin. Configuration hints must be upgraded to signed records first.
//! Pins survive disconnects so gossipsub can reconnect still-required peers.

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

/// An operator-configured reason to retain a peer in gossip.
///
/// These reasons are independent of committee membership and of each other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConfiguredPeerKind {
    /// A peer explicitly trusted by the operator.
    Trusted,
    /// A configured discovery bootstrap peer.
    Bootstrap,
    /// A peer explicitly configured for this network.
    Explicit,
}

/// A change to apply to the gossipsub explicit-peer set.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MeshPeerChange<Id> {
    /// Remove an identity with no remaining retention reason.
    Remove(Id),
    /// Add a verified identity required by current policy.
    Add(Id),
}

/// Whether the domain-to-transport mapping came from a verified record.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MeshPeerIdentity<Id> {
    /// A signature-verified network identity.
    Verified(Id),
    /// A configured hint or an unresolved connection, ineligible for a mesh pin.
    Unverified(Id),
}

/// Operator reasons and the last set applied to gossipsub.
#[derive(Debug)]
pub(crate) struct MeshPolicy<Key, Id> {
    /// Independent configured reasons, keyed by domain identity rather than transport key.
    configured: HashMap<Key, HashSet<ConfiguredPeerKind>>,
    /// Verified transport identities currently installed as explicit peers.
    explicit: HashSet<Id>,
}

impl<Key, Id> Default for MeshPolicy<Key, Id> {
    fn default() -> Self {
        Self { configured: HashMap::new(), explicit: HashSet::new() }
    }
}

impl<Key: Eq + Hash, Id: Copy + Eq + Hash> MeshPolicy<Key, Id> {
    /// Add one configured reason without replacing any other reason.
    pub(crate) fn configure(&mut self, key: Key, kind: ConfiguredPeerKind) {
        self.configured.entry(key).or_default().insert(kind);
    }

    /// Remove one reason and discard empty entries, making repeated reloads bounded.
    pub(crate) fn remove_configured(&mut self, key: &Key, kind: ConfiguredPeerKind) {
        let _ = self.configured.get_mut(key).map(|reasons| reasons.remove(&kind));
        self.configured.retain(|_, reasons| !reasons.is_empty());
    }

    /// Whether an identity has at least one configured retention reason.
    pub(crate) fn is_configured(&self, key: &Key) -> bool {
        self.configured.contains_key(key)
    }

    /// Whether a particular configured reason remains active.
    pub(crate) fn has_kind(&self, key: &Key, kind: ConfiguredPeerKind) -> bool {
        self.configured.get(key).is_some_and(|reasons| reasons.contains(&kind))
    }

    /// The set last applied to gossipsub, for call-site regression tests.
    #[cfg(test)]
    pub(crate) fn explicit_peer_ids(&self) -> HashSet<Id> {
        self.explicit.clone()
    }

    /// Apply the latest verified mappings and committee view as one policy snapshot.
    ///
    /// Exclude unverified mappings. Recompute the union rather than
    /// remembering historical classifications. Remove obsolete identities before adding
    /// replacements, and retain an identity while any required key still resolves to it.
    pub(crate) fn reconcile(
        &mut self,
        mappings: impl IntoIterator<Item = (Key, MeshPeerIdentity<Id>)>,
        in_committee: impl Fn(&Key) -> bool,
    ) -> Vec<MeshPeerChange<Id>> {
        let desired: HashSet<_> = mappings
            .into_iter()
            .filter_map(|(key, identity)| match identity {
                MeshPeerIdentity::Verified(id)
                    if self.is_configured(&key) || in_committee(&key) =>
                {
                    Some(id)
                }
                MeshPeerIdentity::Verified(_) | MeshPeerIdentity::Unverified(_) => None,
            })
            .collect();
        let changes = self
            .explicit
            .difference(&desired)
            .copied()
            .map(MeshPeerChange::Remove)
            .chain(desired.difference(&self.explicit).copied().map(MeshPeerChange::Add))
            .collect();
        self.explicit = desired;
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test domain keys, kept separate from transport identities.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Authority(u64);

    /// Test transport identity.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Identity(u64);

    /// A verified record for a test domain key and transport identity.
    fn record(key: Authority, id: Identity) -> (Authority, MeshPeerIdentity<Identity>) {
        (key, MeshPeerIdentity::Verified(id))
    }

    /// Rotation keeps the three-slot union and removes every historical identity.
    #[test]
    fn rotations_do_not_accumulate_explicit_peers() {
        let mut policy = MeshPolicy::default();
        (1..32).for_each(|epoch| {
            let keys = [Authority(epoch), Authority(epoch + 1), Authority(epoch + 2)];
            let records = (0..35).map(|key| record(Authority(key), Identity(key)));
            let changes = policy.reconcile(records, |key| keys.contains(key));
            assert_eq!(policy.explicit_peer_ids(), HashSet::from(keys.map(|key| Identity(key.0))));
            assert_eq!(changes.len(), if epoch == 1 { 3 } else { 2 });
        });
    }

    /// Removing a classification preserves every overlapping reason, including committee duty.
    #[test]
    fn overlapping_reasons_survive_individual_removal() {
        let mut policy = MeshPolicy::default();
        let key = Authority(1);
        let records = [record(key, Identity(1))];
        let kinds = [
            ConfiguredPeerKind::Trusted,
            ConfiguredPeerKind::Bootstrap,
            ConfiguredPeerKind::Explicit,
        ];
        kinds.into_iter().for_each(|kind| policy.configure(key, kind));
        assert!(policy.has_kind(&key, ConfiguredPeerKind::Trusted));
        assert_eq!(policy.reconcile(records, |_| false), vec![MeshPeerChange::Add(Identity(1))]);
        kinds.into_iter().take(2).for_each(|kind| {
            policy.remove_configured(&key, kind);
            assert!(policy.is_configured(&key));
            assert!(policy.reconcile(records, |_| false).is_empty());
        });
        policy.remove_configured(&key, ConfiguredPeerKind::Explicit);
        assert!(policy.reconcile(records, |_| true).is_empty());
        assert!(!policy.is_configured(&key));
        assert_eq!(policy.reconcile(records, |_| false), vec![MeshPeerChange::Remove(Identity(1))]);
    }

    /// A network-key replacement removes the old identity before pinning the new one.
    #[test]
    fn mapping_replacement_removes_obsolete_identity() {
        let mut policy = MeshPolicy::default();
        let key = Authority(1);
        policy.configure(key, ConfiguredPeerKind::Trusted);
        let _ = policy.reconcile([record(key, Identity(1))], |_| false);
        assert_eq!(
            policy.reconcile([record(key, Identity(2))], |_| false),
            vec![MeshPeerChange::Remove(Identity(1)), MeshPeerChange::Add(Identity(2))]
        );
        assert!(policy.reconcile([record(key, Identity(2))], |_| false).is_empty());
    }

    /// Policy alone cannot pin an unresolved identity; a later verified record can.
    #[test]
    fn grace_without_verified_mapping_has_no_pin() {
        let mut policy = MeshPolicy::default();
        let key = Authority(1);
        policy.configure(key, ConfiguredPeerKind::Bootstrap);
        assert!(policy
            .reconcile([(key, MeshPeerIdentity::Unverified(Identity(7)))], |_| true)
            .is_empty());
        assert!(policy.reconcile([], |_| true).is_empty());
        assert_eq!(
            policy.reconcile([record(key, Identity(1))], |_| false),
            vec![MeshPeerChange::Add(Identity(1))]
        );
        assert_eq!(policy.reconcile([], |_| true), vec![MeshPeerChange::Remove(Identity(1))]);
    }

    /// A transport identity remains pinned while another required domain key still uses it.
    #[test]
    fn shared_identity_retains_remaining_reason() {
        let mut policy = MeshPolicy::default();
        policy.configure(Authority(1), ConfiguredPeerKind::Explicit);
        let records = [record(Authority(1), Identity(1)), record(Authority(2), Identity(1))];
        let _ = policy.reconcile(records, |_| false);
        policy.remove_configured(&Authority(1), ConfiguredPeerKind::Explicit);
        assert!(policy.reconcile(records, |key| *key == Authority(2)).is_empty());
        assert_eq!(policy.reconcile(records, |_| false), vec![MeshPeerChange::Remove(Identity(1))]);
    }

    /// Repeated configuration reloads release both the reason entries and gossip privileges.
    #[test]
    fn reloads_do_not_accumulate_configured_peers() {
        let mut policy = MeshPolicy::default();
        (0..32).for_each(|generation| {
            let key = Authority(generation);
            let id = Identity(generation);
            policy.configure(key, ConfiguredPeerKind::Explicit);
            policy.configure(key, ConfiguredPeerKind::Explicit);
            assert_eq!(
                policy.reconcile([record(key, id)], |_| false),
                vec![MeshPeerChange::Add(id)]
            );
            policy.remove_configured(&key, ConfiguredPeerKind::Explicit);
            policy.remove_configured(&key, ConfiguredPeerKind::Explicit);
            assert_eq!(
                policy.reconcile([record(key, id)], |_| false),
                vec![MeshPeerChange::Remove(id)]
            );
            assert!(policy.configured.is_empty());
            assert!(policy.explicit_peer_ids().is_empty());
        });
    }
}
