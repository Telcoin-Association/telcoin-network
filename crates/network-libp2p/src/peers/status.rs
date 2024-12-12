//! Status of the peer.

use libp2p::Multiaddr;
use serde::{ser::SerializeStruct as _, Serialize, Serializer};
use std::{net::IpAddr, time::Instant};

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
    /// We are currently dialing this peer.
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
