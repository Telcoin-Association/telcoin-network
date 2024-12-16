//! Status of the peer.
//!
//! The connection status and sync status for the peer.

use libp2p::Multiaddr;
use serde::{ser::SerializeStruct as _, Serialize, Serializer};
use std::{net::IpAddr, time::Instant};
use tn_types::{BlockHash, Epoch};

/// Connection status of the peer.
#[derive(Debug, Clone, Default)]
pub enum ConnectionStatus {
    /// The peer is connected.
    Connected {
        /// The multiaddr that we are connected via.
        multiaddr: Multiaddr,
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
        last_seen: Instant,
    },
    /// The peer is banned and disconnected.
    Banned {
        /// The moment when the peer was banned.
        last_seen: Instant,
    },
    /// The peer is being dialed.
    Dialing {
        /// The last known peer connection.
        last_seen: Instant,
    },
    /// The connection status has not been specified.
    #[default]
    Unknown,
}

/// Serialization for http requests.
impl Serialize for ConnectionStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("connection_status", 6)?;
        match self {
            ConnectionStatus::Connected { num_in, num_out, multiaddr } => {
                s.serialize_field("multiaddr", multiaddr)?;
                s.serialize_field("status", "connected")?;
                s.serialize_field("connections_in", num_in)?;
                s.serialize_field("connections_out", num_out)?;
                s.serialize_field("last_seen", &0)?;
                s.end()
            }
            ConnectionStatus::Disconnecting { .. } => {
                s.serialize_field("status", "disconnecting")?;
                s.serialize_field("connections_in", &0)?;
                s.serialize_field("connections_out", &0)?;
                s.serialize_field("last_seen", &0)?;
                s.end()
            }
            ConnectionStatus::Disconnected { last_seen } => {
                s.serialize_field("status", "disconnected")?;
                s.serialize_field("connections_in", &0)?;
                s.serialize_field("connections_out", &0)?;
                s.serialize_field("last_seen", &last_seen.elapsed().as_secs())?;
                s.serialize_field("banned_ips", &Vec::<IpAddr>::new())?;
                s.end()
            }
            ConnectionStatus::Banned { last_seen } => {
                s.serialize_field("status", "banned")?;
                s.serialize_field("connections_in", &0)?;
                s.serialize_field("connections_out", &0)?;
                s.serialize_field("last_seen", &last_seen.elapsed().as_secs())?;
                s.end()
            }
            ConnectionStatus::Dialing { last_seen } => {
                s.serialize_field("status", "dialing")?;
                s.serialize_field("connections_in", &0)?;
                s.serialize_field("connections_out", &0)?;
                s.serialize_field("last_seen", &last_seen.elapsed().as_secs())?;
                s.end()
            }
            ConnectionStatus::Unknown => {
                s.serialize_field("status", "unknown")?;
                s.serialize_field("connections_in", &0)?;
                s.serialize_field("connections_out", &0)?;
                s.serialize_field("last_seen", &0)?;
                s.end()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
/// The current sync status of the peer.
pub enum SyncStatus {
    /// At the current state as our node or ahead of us.
    Synced { info: SyncInfo },
    /// The peer has greater knowledge about the canonical chain than we do.
    Advanced { info: SyncInfo },
    /// Is behind our current head and not useful for block downloads.
    Behind { info: SyncInfo },
    /// This peer is in an incompatible network.
    IrrelevantPeer,
    /// Not currently known as a STATUS handshake has not occurred.
    Unknown,
}

/// Relevant information pertaining to the peer's sync status.
#[derive(Clone, Debug, Serialize)]
pub struct SyncInfo {
    /// The hash of the last executed block corresponding to the epoch.
    pub execution: BlockHash,
    /// The hash of the last consensus header for the epoch.
    pub consensus: BlockHash,
    /// The latest epoch this peer has synced.
    pub epoch: Epoch,
}

impl std::cmp::PartialEq for SyncStatus {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (SyncStatus::Synced { .. }, SyncStatus::Synced { .. })
                | (SyncStatus::Advanced { .. }, SyncStatus::Advanced { .. })
                | (SyncStatus::Behind { .. }, SyncStatus::Behind { .. })
                | (SyncStatus::IrrelevantPeer, SyncStatus::IrrelevantPeer)
                | (SyncStatus::Unknown, SyncStatus::Unknown)
        )
    }
}

impl SyncStatus {
    /// Returns true if the peer has advanced knowledge of the chain.
    pub fn is_advanced(&self) -> bool {
        matches!(self, SyncStatus::Advanced { .. })
    }

    /// Returns true if the peer is up to date with the current chain.
    pub fn is_synced(&self) -> bool {
        matches!(self, SyncStatus::Synced { .. })
    }

    /// Returns true if the peer is behind the current chain.
    pub fn is_behind(&self) -> bool {
        matches!(self, SyncStatus::Behind { .. })
    }

    /// Updates the peer's sync status, returning whether the status transitioned.
    ///
    /// E.g. returns `true` if the state changed from `Synced` to `Advanced`, but not if
    /// the status remained `Synced` with different `SyncInfo` within.
    pub fn update(&mut self, new_state: SyncStatus) -> bool {
        let changed_status = *self != new_state;
        *self = new_state;
        changed_status
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            SyncStatus::Advanced { .. } => "Advanced",
            SyncStatus::Behind { .. } => "Behind",
            SyncStatus::Synced { .. } => "Synced",
            SyncStatus::Unknown => "Unknown",
            SyncStatus::IrrelevantPeer => "Irrelevant",
        }
    }
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
