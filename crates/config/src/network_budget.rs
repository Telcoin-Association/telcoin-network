//! Opt-in allocation of established QUIC resources across process-lifetime swarms.

use serde::{Deserialize, Serialize};
use std::{
    fmt,
    num::{NonZeroU32, NonZeroU64},
};

/// Operator-selected ceilings for all primary and worker swarms in one process.
///
/// These bound established connections, incoming bidirectional stream capacity, and advertised
/// connection receive credit. They do not bound RSS, pending handshakes, outbound streams, tasks,
/// application buffers, or CPU. Production values require workload calibration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkProcessBudget {
    /// Expected primary plus configured worker swarms, including currently inactive workers.
    swarm_count: NonZeroU32,
    /// Total established connections across all swarms, directions, and peer classes.
    max_established_connections: NonZeroU32,
    /// Established connections allowed for one identity in each swarm.
    max_established_connections_per_peer: NonZeroU32,
    /// Total incoming bidirectional stream capacity across established connections.
    max_inbound_streams: NonZeroU64,
    /// Total advertised connection receive credit in bytes, not resident memory.
    max_receive_credit_bytes: NonZeroU64,
}

/// Equal, non-borrowable allocation for one swarm and each of its connections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SwarmNetworkBudget {
    /// Maximum established connections in this swarm.
    connections: u32,
    /// Maximum established connections for one peer in this swarm.
    connections_per_peer: u32,
    /// Maximum incoming bidirectional streams per connection.
    streams_per_connection: u32,
    /// Maximum advertised receive credit per connection, in bytes.
    receive_credit_per_connection: u32,
}

/// Invalid process allocation or a mismatch with the node's actual swarm topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkBudgetError {
    /// A configured resource cannot provide even one unit to each allocation.
    InsufficientBudget(&'static str),
    /// The configured topology differs from the primary plus every configured worker.
    SwarmCountMismatch {
        /// Swarms declared in the process budget.
        configured: u32,
        /// Swarms that the node will create.
        actual: usize,
    },
}

impl fmt::Display for NetworkBudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientBudget(field) => write!(f, "process_budget.{field} is too small for the allocation"),
            Self::SwarmCountMismatch { configured, actual } => write!(f, "process_budget.swarm_count is {configured}, but the node configures {actual} swarms"),
        }
    }
}

impl std::error::Error for NetworkBudgetError {}

impl NetworkProcessBudget {
    /// Check the full process topology before any primary or worker swarm is started.
    pub fn validate_swarm_count(&self, actual: usize) -> Result<(), NetworkBudgetError> {
        if u32::try_from(actual) == Ok(self.swarm_count.get()) {
            self.allocate().map(|_| ())
        } else {
            Err(NetworkBudgetError::SwarmCountMismatch {
                configured: self.swarm_count.get(),
                actual,
            })
        }
    }

    /// Divide resources without rounding up or borrowing another swarm's reservation.
    ///
    /// If `S` is the swarm count and `C` the process connection ceiling, each swarm admits at most
    /// `floor(C / S)` connections. Stream and credit ceilings are divided by the resulting total
    /// admitted connections. Remainders stay unused, and transport values saturate at `u32::MAX`.
    pub fn allocate(&self) -> Result<SwarmNetworkBudget, NetworkBudgetError> {
        let connections =
            NonZeroU32::new(self.max_established_connections.get() / self.swarm_count.get())
                .ok_or(NetworkBudgetError::InsufficientBudget("max_established_connections"))?
                .get();
        let total_connections = u64::from(connections) * u64::from(self.swarm_count.get());
        let divide = |value: NonZeroU64, field| {
            let per_connection = value.get() / total_connections;
            if per_connection == 0 {
                Err(NetworkBudgetError::InsufficientBudget(field))
            } else {
                Ok(u32::try_from(per_connection).unwrap_or(u32::MAX))
            }
        };
        Ok(SwarmNetworkBudget {
            connections,
            connections_per_peer: self.max_established_connections_per_peer.get().min(connections),
            streams_per_connection: divide(self.max_inbound_streams, "max_inbound_streams")?,
            receive_credit_per_connection: divide(
                self.max_receive_credit_bytes,
                "max_receive_credit_bytes",
            )?,
        })
    }
}

impl SwarmNetworkBudget {
    /// Maximum established connections for this swarm, including trusted peers.
    pub fn connections(&self) -> u32 {
        self.connections
    }

    /// Maximum established connections for one identity, across both directions.
    pub fn connections_per_peer(&self) -> u32 {
        self.connections_per_peer
    }

    /// Maximum incoming bidirectional stream capacity per connection.
    pub fn streams_per_connection(&self) -> u32 {
        self.streams_per_connection
    }

    /// Maximum advertised connection receive credit per connection, in bytes.
    pub fn receive_credit_per_connection(&self) -> u32 {
        self.receive_credit_per_connection
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse operator input without bypassing serde's nonzero checks.
    fn budget(
        swarms: u32,
        connections: u32,
        streams: u64,
        credit: u64,
    ) -> Result<NetworkProcessBudget, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "swarm_count": swarms,
            "max_established_connections": connections,
            "max_established_connections_per_peer": 8,
            "max_inbound_streams": streams,
            "max_receive_credit_bytes": credit
        }))
    }

    /// All worker counts share one process ceiling; division never rounds a remainder up.
    #[test]
    fn allocations_stay_within_process_ceilings() -> Result<(), serde_json::Error> {
        (1..=32).try_for_each(|swarms| {
            assert_eq!(
                budget(swarms, 97, 10_003, 1_000_003)?.allocate().map(|allocated| {
                    let total = u64::from(allocated.connections()) * u64::from(swarms);
                    assert!(total <= 97);
                    assert!(total * u64::from(allocated.streams_per_connection()) <= 10_003);
                    assert!(
                        total * u64::from(allocated.receive_credit_per_connection()) <= 1_000_003
                    );
                    assert!(allocated.connections_per_peer() <= allocated.connections());
                }),
                Ok(())
            );
            Ok(())
        })
    }

    /// Reject impossible allocations and stale topology declarations before networking starts.
    #[test]
    fn rejects_undersized_budgets_and_topology_changes() -> Result<(), serde_json::Error> {
        assert_eq!(
            budget(3, 2, 100, 100)?.allocate(),
            Err(NetworkBudgetError::InsufficientBudget("max_established_connections"))
        );
        assert_eq!(
            budget(2, 8, 7, 100)?.allocate(),
            Err(NetworkBudgetError::InsufficientBudget("max_inbound_streams"))
        );
        assert_eq!(
            budget(2, 8, 100, 7)?.allocate(),
            Err(NetworkBudgetError::InsufficientBudget("max_receive_credit_bytes"))
        );
        assert_eq!(
            budget(2, 8, 100, 100)?.validate_swarm_count(3),
            Err(NetworkBudgetError::SwarmCountMismatch { configured: 2, actual: 3 })
        );
        assert!(budget(0, 8, 100, 100).is_err());
        assert!(budget(2, 0, 100, 100).is_err());
        assert!(budget(2, 8, 0, 100).is_err());
        assert!(budget(2, 8, 100, 0).is_err());
        Ok(())
    }

    /// Large operator ceilings cannot truncate into small transport limits.
    #[test]
    fn large_budgets_saturate_transport_width() -> Result<(), serde_json::Error> {
        assert_eq!(
            budget(1, 1, u64::MAX, u64::MAX)?.allocate().map(|allocated| (
                allocated.connections(),
                allocated.connections_per_peer(),
                allocated.streams_per_connection(),
                allocated.receive_credit_per_connection()
            )),
            Ok((1, 1, u32::MAX, u32::MAX))
        );
        Ok(())
    }
}
