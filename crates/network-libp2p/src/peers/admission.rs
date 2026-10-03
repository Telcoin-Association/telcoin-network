//! Connection admission reservations owned by the peer manager.
//!
//! A distinct identity consumes one slot across reservations, established connections and
//! connections awaiting closure. Each connection ID still owns its own reservation so a later
//! composed-behaviour denial cannot release another connection's slot. Anonymous inbound
//! handshakes are bounded separately by libp2p's connection-limits behaviour; identity admission
//! occurs in the authenticated callback before any peer state is committed.
//!
//! Ordinary admissions stop at the configured directional population limit. Both established
//! callbacks check the limit before the connection is accepted, so admitted peers are never
//! disconnected as excess at establishment. Committee and operator-allowlisted identities retain
//! their exemption, so population can additionally contain those authenticated identities
//! (including draining members after rotation). Their reservations are still counted when
//! admitting ordinary peers. The composed connection-limits behaviour owns the independent
//! absolute connection bound, including privileged and duplicate connections.
//!
//! When `peer_exchange_at_capacity` is enabled (bootstrap nodes), a new inbound identity at
//! capacity is accepted as a hand-off instead of refused: it holds no population slot, and once
//! established it is disconnected with peer exchange and temporarily banned, so the newcomer
//! leaves with other peers to try. Outbound connections are always refused at capacity.

use super::PeerManager;
use libp2p::{
    core::Endpoint,
    swarm::{ConnectionDenied, ConnectionId},
    PeerId,
};
use std::collections::HashSet;
use tracing::debug;

/// Ownership of a population reservation through the connection lifecycle.
pub(super) enum ConnectionAdmission {
    /// Accepted by the peer manager, awaiting acceptance by the composed behaviour stack.
    Reserved(PeerId),
    /// Accepted by every behaviour, retained until actual connection closure.
    Established(PeerId),
    /// New inbound identity at capacity, accepted only to hand it other peers to try.
    ///
    /// Holds no population slot. On establishment the peer is disconnected with peer exchange
    /// instead of committed; the entry is retained until failure or closure.
    HandOff,
}

impl ConnectionAdmission {
    /// The identity whose population slot this connection shares, if it holds one.
    ///
    /// Hand-off connections hold no slot, so they never count toward admission.
    fn population_slot(&self) -> Option<PeerId> {
        match self {
            Self::Reserved(peer_id) => Some(*peer_id),
            Self::Established(peer_id) => Some(*peer_id),
            Self::HandOff => None,
        }
    }
}

/// The peer manager refused a new identity because its directional population limit is reached.
///
/// Carried as the cause of [`ConnectionDenied`] so the swarm event loop can count and log
/// capacity refusals separately from libp2p connection-limit denials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerCapacityReached;

impl std::fmt::Display for PeerCapacityReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer population capacity reached")
    }
}

impl std::error::Error for PeerCapacityReached {}

impl PeerManager {
    /// Reserve capacity before accepting a connection, without committing peer state.
    ///
    /// Population is the distinct identities across connected peers and this manager's
    /// reservations; a `Dialing` status alone does not count. Equality denies a new identity: the
    /// last available slot is admitted at limit minus one. Outbound peers use
    /// `max_outbound_dialing_peers` (checked before dialing and again at establishment), inbound
    /// peers use `max_peers`, and committee/operator-allowlisted peers retain their exemption.
    /// Existing identities can add connections at the limit; connection-limits owns the separate
    /// absolute connection cap.
    ///
    /// A new inbound identity at capacity is accepted as a hand-off when
    /// `peer_exchange_at_capacity` is enabled; otherwise it is refused with
    /// [`PeerCapacityReached`].
    pub(in crate::peers) fn reserve_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        direction: Endpoint,
    ) -> Result<(), ConnectionDenied> {
        let population: HashSet<_> = self
            .peers
            .connected_peer_ids()
            .chain(
                self.connection_admissions
                    .values()
                    .filter_map(ConnectionAdmission::population_slot),
            )
            .collect();
        let limit = if direction == Endpoint::Dialer {
            self.config.max_outbound_dialing_peers()
        } else {
            self.config.max_peers()
        };
        let at_capacity = !population.contains(&peer_id)
            && !self.peer_is_important(&peer_id)
            && population.len() >= limit;
        let hand_off = direction == Endpoint::Listener && self.config.peer_exchange_at_capacity();

        match () {
            _ if at_capacity && !hand_off => {
                debug!(target: "peer-manager", ?peer_id, ?direction, population = population.len(), limit, "peer population capacity reached - refusing connection");
                Err(ConnectionDenied::new(PeerCapacityReached))
            }
            _ if at_capacity => {
                debug!(target: "peer-manager", ?peer_id, population = population.len(), limit, "peer population capacity reached - accepting for peer exchange hand-off");
                self.connection_admissions.insert(connection_id, ConnectionAdmission::HandOff);
                Ok(())
            }
            _ => {
                self.connection_admissions
                    .insert(connection_id, ConnectionAdmission::Reserved(peer_id));
                Ok(())
            }
        }
    }

    /// Whether this connection was accepted only for a peer exchange hand-off.
    pub(in crate::peers) fn is_hand_off(&self, connection_id: &ConnectionId) -> bool {
        self.connection_admissions
            .get(connection_id)
            .is_some_and(|admission| matches!(admission, ConnectionAdmission::HandOff))
    }

    /// Commit only on `ConnectionEstablished`, after all behaviours have accepted the connection.
    ///
    /// Hand-off connections are never committed; see [`Self::is_hand_off`].
    pub(in crate::peers) fn commit_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
    ) {
        self.connection_admissions.insert(connection_id, ConnectionAdmission::Established(peer_id));
    }

    /// Release exactly this connection's ownership on failure or closure; repeated events are
    /// inert.
    pub(in crate::peers) fn release_connection(&mut self, connection_id: ConnectionId) {
        self.connection_admissions.remove(&connection_id);
    }
}
