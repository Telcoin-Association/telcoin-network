//! Status of the peer.
//!
//! The connection status and sync status for the peer.

use super::types::ConnectionDirection;
use libp2p::Multiaddr;
use std::time::Instant;

/// Connection status of the peer.
#[derive(Debug, Clone, Default)]
pub(super) enum ConnectionStatus {
    /// The peer is connected.
    Connected {
        /// The number of ingoing connections.
        num_in: u8,
        /// The number of outgoing connections.
        num_out: u8,
    },
    /// The peer is in the process of disconnecing.
    Disconnecting {
        // Indicates if the peer is banned after disconnection.
        banned: bool,
    },
    /// The peer has disconnected.
    Disconnected {
        /// The last known connected instant.
        instant: Instant,
    },
    /// The peer is banned and disconnected.
    Banned {
        /// The moment when the peer was banned.
        instant: Instant,
    },
    /// The peer is being dialed.
    Dialing {
        /// The last known peer connection.
        instant: Instant,
    },
    /// The connection status has not been specified.
    #[default]
    Unknown,
}

impl ConnectionStatus {
    /// Matches the connection status.
    pub(super) fn is_banned(&self) -> bool {
        matches!(self, Self::Banned { .. })
    }

    /// Matches the connection status.
    pub(super) fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }

    /// Matches the connection status.
    pub(super) fn is_dialing(&self) -> bool {
        matches!(self, Self::Dialing { .. })
    }

    /// Matches the connection status if the peer is already connected or dialing.
    pub(super) fn is_connected_or_dialing(&self) -> bool {
        self.is_connected() || self.is_dialing()
    }
}

/// Enum for managing a peer's transition to new connection status.
#[derive(Debug)]
pub(super) enum NewConnectionStatus {
    /// A peer has established a connection.
    Connected {
        /// The multiaddress associated with the connection.
        multiaddr: Multiaddr,
        /// The direction, incoming/outgoing.
        direction: ConnectionDirection,
    },
    /// The peer is being disconnected.
    Disconnecting {
        /// Whether the peer should be banned after the disconnect occurs.
        banned: bool,
    },
    /// A peer is being dialed.
    Dialing,
    /// The peer was fully disconnected from this node.
    Disconnected,
    /// The peer was banned.
    Banned,
    /// The peer was unbanned.
    Unbanned,
}

impl NewConnectionStatus {
    /// Returns true if this status is valid as an initial state for a previously unknown peer.
    ///
    /// Some connection states are only valid for previously tracked peers (like being banned
    /// or having an unknown status). Other states  occur during peer discovery, such as
    /// establishing a new connection or initiating a dial.
    ///
    /// Edge cases like disconnecting or instant disconnection also occur during the
    /// discovery process, such as when a peer responds with a different ID than expected.
    pub(super) fn valid_initial_state(&self) -> bool {
        matches!(
            self,
            // valid initial states for unknown peers
            NewConnectionStatus::Connected { .. }          // new connection established
                | NewConnectionStatus::Disconnecting { .. }// edge case: disconnecting during discovery
                | NewConnectionStatus::Dialing
                | NewConnectionStatus::Disconnected // edge case: instant disconnect
        )
    }
}
