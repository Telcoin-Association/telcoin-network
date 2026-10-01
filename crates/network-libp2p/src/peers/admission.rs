//! Connection admission reservations owned by the peer manager.
//!
//! A distinct identity consumes one slot across reservations, established connections and
//! connections awaiting closure. Each connection ID still owns its own reservation so a later
//! composed-behaviour denial cannot release another connection's slot. Anonymous inbound
//! handshakes are bounded separately by libp2p's connection-limits behaviour; identity admission
//! occurs in the authenticated callback before any peer state is committed.
//!
//! Ordinary admissions stop at the configured directional population limit. Committee and
//! operator-allowlisted identities retain their exemption, so population can additionally contain
//! those authenticated identities (including draining members after rotation). Their reservations
//! are still counted when admitting ordinary peers. The composed connection-limits behaviour owns
//! the independent absolute connection bound, including privileged and duplicate connections.

use super::PeerManager;
use libp2p::{
    core::Endpoint,
    swarm::{ConnectionDenied, ConnectionId},
    PeerId,
};
use std::collections::HashSet;

/// Ownership of a population reservation through the connection lifecycle.
pub(super) enum ConnectionAdmission {
    /// Accepted by the peer manager, awaiting acceptance by the composed behaviour stack.
    Reserved(PeerId),
    /// Accepted by every behaviour, retained until actual connection closure.
    Established(PeerId),
}

impl ConnectionAdmission {
    /// The identity whose population slot this connection shares.
    fn peer_id(&self) -> PeerId {
        match self {
            Self::Reserved(peer_id) => *peer_id,
            Self::Established(peer_id) => *peer_id,
        }
    }
}

impl PeerManager {
    /// Reserve capacity before accepting a connection, without committing peer state.
    ///
    /// Equality denies a new identity: the last available slot is admitted at limit minus one.
    /// Outbound peers use `max_outbound_dialing_peers`, inbound peers use `max_peers`, and
    /// committee/operator-allowlisted peers retain their exemption. Existing identities can
    /// add connections at the limit; connection-limits owns the separate absolute connection cap.
    pub(in crate::peers) fn reserve_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        direction: Endpoint,
    ) -> Result<(), ConnectionDenied> {
        let population: HashSet<_> = self
            .peers
            .connected_peer_ids()
            .chain(self.connection_admissions.values().map(ConnectionAdmission::peer_id))
            .collect();
        let limit = if direction == Endpoint::Dialer {
            self.config.max_outbound_dialing_peers()
        } else {
            self.config.max_peers()
        };
        if !population.contains(&peer_id)
            && !self.peer_is_important(&peer_id)
            && population.len() >= limit
        {
            Err(ConnectionDenied::new("peer population capacity reached"))
        } else {
            self.connection_admissions
                .insert(connection_id, ConnectionAdmission::Reserved(peer_id));
            Ok(())
        }
    }

    /// Commit only on `ConnectionEstablished`, after all behaviours have accepted the connection.
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
