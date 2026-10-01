//! Explicit deployment limits for established connections by observed source.

use serde::{Deserialize, Serialize};

/// Opt-in established-connection limits with no production defaults.
///
/// All fields are required. Values must come from the deployment's peer, reconnect,
/// shared-NAT, and process resource measurements. Every participating swarm shares
/// one instance of the resulting runtime budget.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAdmissionConfig {
    /// Maximum simultaneous established or reserved connections in the process.
    max_connections: usize,
    /// Maximum simultaneous connections for one authenticated peer identity.
    max_connections_per_peer: usize,
    /// Maximum simultaneous connections sharing one observed address.
    max_connections_per_address: usize,
    /// Maximum simultaneous connections sharing one observed prefix.
    max_connections_per_prefix: usize,
    /// Maximum distinct observed addresses retained in the accounting table.
    max_sources: usize,
    /// IPv4 prefix length, in the inclusive range 0 through 32.
    ipv4_prefix_length: u8,
    /// IPv6 prefix length, in the inclusive range 0 through 128.
    ipv6_prefix_length: u8,
}

impl SourceAdmissionConfig {
    /// Return the process-wide connection ceiling.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }
    /// Return the per-identity connection ceiling.
    pub fn max_connections_per_peer(&self) -> usize {
        self.max_connections_per_peer
    }
    /// Return the per-address connection ceiling.
    pub fn max_connections_per_address(&self) -> usize {
        self.max_connections_per_address
    }
    /// Return the per-prefix connection ceiling.
    pub fn max_connections_per_prefix(&self) -> usize {
        self.max_connections_per_prefix
    }
    /// Return the distinct-source table ceiling.
    pub fn max_sources(&self) -> usize {
        self.max_sources
    }
    /// Return the deployment-selected IPv4 and IPv6 prefix lengths.
    pub fn prefix_lengths(&self) -> (u8, u8) {
        (self.ipv4_prefix_length, self.ipv6_prefix_length)
    }
}
