//! Established-connection share of the peers without privilege.
//!
//! The `connection_limits` behaviour bounds every established connection of a swarm,
//! privileged or not, so free identities alone could fill that total and lock committee
//! members out. The peer manager holds peers without privilege to a public share instead: the
//! swarm total minus the privileged reserve, [`PRIVILEGED_ESTABLISHED_CONNECTION_RESERVE`] of the
//! default [`MAX_ESTABLISHED_CONNECTIONS`] scaled to the total, and at most
//! [`MAX_ESTABLISHED_CONNECTIONS_PER_PUBLIC_PEER`] connections each. Privileged peers take no
//! slot, so the reserve stays free to them whatever free identities do.

use crate::consensus::{
    MAX_ESTABLISHED_CONNECTIONS, MAX_ESTABLISHED_CONNECTIONS_PER_PUBLIC_PEER,
    PRIVILEGED_ESTABLISHED_CONNECTION_RESERVE,
};
use libp2p::{swarm::ConnectionId, PeerId};
use std::collections::HashMap;

/// The established connections of a swarm with `total_established` in total that peers without
/// privilege can never hold.
///
/// The reserve keeps the split of the default derivation at every total: it is the total times
/// [`PRIVILEGED_ESTABLISHED_CONNECTION_RESERVE`] / [`MAX_ESTABLISHED_CONNECTIONS`], rounded down.
/// So the default 128 keeps 50, a total of 12 keeps 4 and a total of 2 keeps none. The quotient
/// is never above the total, so the `u32::MAX` fallback never applies.
pub(crate) fn privileged_reserve(total_established: u32) -> u32 {
    u32::try_from(
        u64::from(total_established)
            .saturating_mul(u64::from(PRIVILEGED_ESTABLISHED_CONNECTION_RESERVE))
            / u64::from(MAX_ESTABLISHED_CONNECTIONS),
    )
    .unwrap_or(u32::MAX)
}

/// Established-connection share of the peers without privilege in one swarm.
///
/// Each slot is keyed by the libp2p `ConnectionId`, so a slot is held per connection, not per
/// peer: an established connection hook takes the slot when it accepts a peer without
/// privilege, and the slot is freed when that connection closes or the swarm reports its
/// failure. Both directions use the same share. A second reserve or release of the same id
/// changes nothing.
#[derive(Debug, Clone)]
pub(crate) struct PublicConnectionShare {
    /// Established connections, both directions, that all peers without privilege may hold.
    share: u32,
    /// Established connections one peer without privilege may hold.
    per_peer: u32,
    /// The held slots, keyed by connection id, with the holder.
    held: HashMap<ConnectionId, PeerId>,
    /// Slots held by each holder (removed at zero).
    per_holder: HashMap<PeerId, u32>,
}

impl PublicConnectionShare {
    /// The public share of a swarm whose established-connection bounds are `total_established`
    /// in total and `per_peer_ceiling` per peer.
    ///
    /// The share is the total minus its [`privileged_reserve`] (saturating), and each peer
    /// without privilege may hold at most [`MAX_ESTABLISHED_CONNECTIONS_PER_PUBLIC_PEER`]
    /// connections, never more than the per-peer ceiling. The reserve scales with the total, so
    /// an operator `process_budget` of any size above zero still admits peers without privilege.
    pub(crate) fn of(total_established: u32, per_peer_ceiling: u32) -> Self {
        Self {
            share: total_established.saturating_sub(privileged_reserve(total_established)),
            per_peer: MAX_ESTABLISHED_CONNECTIONS_PER_PUBLIC_PEER.min(per_peer_ceiling),
            held: HashMap::new(),
            per_holder: HashMap::new(),
        }
    }

    /// Hold one slot of the share, keyed by `connection_id`, on behalf of `peer`.
    ///
    /// A connection id that already holds a slot is accepted again without a second count.
    /// Otherwise the slot is refused with [`PublicShareExceeded::Share`] when the share is full
    /// and with [`PublicShareExceeded::PerPeer`] when `peer` already holds its connection limit.
    pub(crate) fn reserve(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
    ) -> Result<(), PublicShareExceeded> {
        let holder_count = self.held_by(&peer);
        match () {
            () if self.held.contains_key(&connection_id) => Ok(()),
            () if self.share_is_full() => Err(PublicShareExceeded::Share),
            () if holder_count >= self.per_peer => Err(PublicShareExceeded::PerPeer),
            () => {
                self.held.insert(connection_id, peer);
                self.per_holder.insert(peer, holder_count.saturating_add(1));
                Ok(())
            }
        }
    }

    /// Free the slot held by `connection_id`; an id without a slot frees nothing.
    pub(crate) fn release(&mut self, connection_id: ConnectionId) {
        let holder = self.held.remove(&connection_id);
        holder.iter().for_each(|holder| self.decrement(holder));
    }

    /// The number of slots that all peers without privilege hold.
    pub(crate) fn held(&self) -> usize {
        self.held.len()
    }

    /// The number of slots that `peer` holds.
    pub(crate) fn held_by(&self, peer: &PeerId) -> u32 {
        self.per_holder.get(peer).copied().unwrap_or(0)
    }

    /// Whether every slot of the share is held.
    fn share_is_full(&self) -> bool {
        u32::try_from(self.held.len()).unwrap_or(u32::MAX) >= self.share
    }

    /// Drop one slot from the count of `holder`, and remove the holder at zero.
    fn decrement(&mut self, holder: &PeerId) {
        let remaining = self.held_by(holder).saturating_sub(1);
        if remaining == 0 {
            self.per_holder.remove(holder);
        } else {
            self.per_holder.insert(*holder, remaining);
        }
    }
}

/// Why the public connection share refused a peer without privilege.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicShareExceeded {
    /// Every slot of the public share is held.
    Share,
    /// The peer already holds its public connection limit
    /// ([`MAX_ESTABLISHED_CONNECTIONS_PER_PUBLIC_PEER`], or a lower per-peer ceiling).
    PerPeer,
}

impl PublicShareExceeded {
    /// The bounded metric reason of this refusal.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Share => "peer_manager_public_share",
            Self::PerPeer => "peer_manager_public_peer_connections",
        }
    }
}

impl std::fmt::Display for PublicShareExceeded {
    /// Write the refusal message.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Share => "public connection share is full",
            Self::PerPeer => "peer without privilege holds its connection limit",
        })
    }
}

impl std::error::Error for PublicShareExceeded {}

/// Unit tests of the public connection share.
#[cfg(test)]
mod tests {
    use super::*;

    /// A test connection id.
    fn id(n: usize) -> ConnectionId {
        ConnectionId::new_unchecked(n)
    }

    /// The default total keeps the committee reserve, and a tiny total keeps a public share.
    #[test]
    fn of_default_total_keeps_the_committee_reserve() {
        let default = PublicConnectionShare::of(128, 8);
        assert_eq!((default.share, default.per_peer), (78, 2));
        let tiny = PublicConnectionShare::of(4, 1);
        assert_eq!((tiny.share, tiny.per_peer), (3, 1));
    }

    /// The reserve keeps the default split of 50 in 128 at every total, rounded down.
    #[test]
    fn privileged_reserve_follows_the_default_split() {
        assert_eq!([128, 12, 2, 0, 256].map(privileged_reserve), [50, 4, 0, 0, 100]);
    }

    /// A small operator total keeps a public share: 12 connections reserve 4 and share 8.
    #[test]
    fn of_small_total_keeps_a_public_share() {
        let mut share = PublicConnectionShare::of(12, 8);
        assert!((1..=8).all(|n| share.reserve(id(n), PeerId::random()).is_ok()));
        assert_eq!(share.held(), 8);
        assert_eq!(share.reserve(id(9), PeerId::random()), Err(PublicShareExceeded::Share));
    }

    /// The slot after the last one is refused until a held slot is released.
    #[test]
    fn share_refuses_the_slot_after_the_last() {
        let mut share = PublicConnectionShare::of(6, 8);
        let (first, second, third) = (PeerId::random(), PeerId::random(), PeerId::random());
        assert_eq!(share.reserve(id(1), first), Ok(()));
        assert_eq!(share.reserve(id(2), first), Ok(()));
        assert_eq!(share.reserve(id(3), second), Ok(()));
        assert_eq!(share.reserve(id(4), second), Ok(()));
        assert_eq!(share.held(), 4);
        assert_eq!(share.reserve(id(5), third), Err(PublicShareExceeded::Share));
        share.release(id(1));
        assert_eq!(share.held(), 3);
        assert_eq!(share.reserve(id(5), third), Ok(()));
        assert_eq!(share.held(), 4);
    }

    /// One peer without privilege holds at most two connections, and a release frees one.
    #[test]
    fn peer_holds_at_most_two_connections() {
        let mut share = PublicConnectionShare::of(128, 8);
        let peer = PeerId::random();
        assert_eq!(share.reserve(id(1), peer), Ok(()));
        assert_eq!(share.reserve(id(2), peer), Ok(()));
        assert_eq!(share.reserve(id(3), peer), Err(PublicShareExceeded::PerPeer));
        assert_eq!(share.held_by(&peer), 2);
        share.release(id(1));
        assert_eq!(share.reserve(id(3), peer), Ok(()));
        assert_eq!(share.held_by(&peer), 2);
    }

    /// A release of an unknown id and a second reserve of a held id change no count.
    #[test]
    fn release_of_an_unknown_id_and_a_double_reserve_change_nothing() {
        let mut share = PublicConnectionShare::of(128, 8);
        let peer = PeerId::random();
        assert_eq!(share.reserve(id(1), peer), Ok(()));
        assert_eq!(share.reserve(id(1), peer), Ok(()));
        assert_eq!((share.held(), share.held_by(&peer)), (1, 1));
        share.release(id(2));
        assert_eq!((share.held(), share.held_by(&peer)), (1, 1));
        share.release(id(1));
        share.release(id(1));
        assert_eq!((share.held(), share.held_by(&peer)), (0, 0));
        assert!(share.per_holder.is_empty());
    }
}
