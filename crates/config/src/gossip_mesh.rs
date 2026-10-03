//! Operator configuration of gossip mesh connectivity.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Mesh degrees shared by the primary and every worker swarm.
///
/// Omission preserves libp2p's default degrees. These values leave publisher
/// authorization, message size, and the privileges of explicit peers unchanged.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GossipMeshConfig {
    /// Desired number of mesh peers per topic.
    target: usize,
    /// Lower degree that triggers grafting additional peers.
    low: usize,
    /// Upper degree that triggers pruning ordinary mesh peers.
    high: usize,
    /// Minimum outbound connections retained in each mesh.
    outbound_min: usize,
}

impl Default for GossipMeshConfig {
    fn default() -> Self {
        Self { target: 6, low: 5, high: 12, outbound_min: 2 }
    }
}

/// An invalid relationship between mesh degrees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GossipMeshError {
    /// The positive low, target, and high degrees are not ordered.
    DegreeOrder,
    /// The outbound floor exceeds the low degree or half of the target.
    OutboundFloor,
}

impl fmt::Display for GossipMeshError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DegreeOrder => "gossip_mesh requires 0 < low <= target <= high",
            Self::OutboundFloor => "gossip_mesh.outbound_min must be <= low and <= target / 2",
        })
    }
}

impl std::error::Error for GossipMeshError {}

impl GossipMeshConfig {
    /// Validate the degree relationships before creating a swarm.
    pub fn validate(&self) -> Result<(), GossipMeshError> {
        if self.low == 0 || self.low > self.target || self.target > self.high {
            Err(GossipMeshError::DegreeOrder)
        } else if self.outbound_min > self.low || self.outbound_min > self.target / 2 {
            // Division avoids overflowing an operator-provided outbound floor.
            Err(GossipMeshError::OutboundFloor)
        } else {
            Ok(())
        }
    }

    /// Return the desired topic mesh degree.
    pub fn target(&self) -> usize {
        self.target
    }

    /// Return the grafting threshold.
    pub fn low(&self) -> usize {
        self.low
    }

    /// Return the pruning threshold.
    pub fn high(&self) -> usize {
        self.high
    }

    /// Return the outbound connection floor.
    pub fn outbound_min(&self) -> usize {
        self.outbound_min
    }
}

#[cfg(test)]
mod tests {
    //! Regression coverage for mesh validation and the shipped profile.

    use super::*;

    /// Legacy configurations retain the existing mesh degrees.
    #[test]
    fn defaults_preserve_mesh_degrees() -> Result<(), GossipMeshError> {
        let mesh = GossipMeshConfig::default();
        assert_eq!((mesh.target(), mesh.low(), mesh.high(), mesh.outbound_min()), (6, 5, 12, 2));
        mesh.validate()
    }

    /// Invalid degrees, including oversized outbound floors, fail before swarm creation.
    #[test]
    fn rejects_invalid_mesh_relationships() {
        [
            (GossipMeshConfig { low: 0, ..Default::default() }, GossipMeshError::DegreeOrder),
            (GossipMeshConfig { low: 7, ..Default::default() }, GossipMeshError::DegreeOrder),
            (GossipMeshConfig { high: 5, ..Default::default() }, GossipMeshError::DegreeOrder),
            (
                GossipMeshConfig { outbound_min: 4, ..Default::default() },
                GossipMeshError::OutboundFloor,
            ),
            (
                GossipMeshConfig { outbound_min: usize::MAX, ..Default::default() },
                GossipMeshError::OutboundFloor,
            ),
        ]
        .into_iter()
        .for_each(|(mesh, error)| assert_eq!(mesh.validate(), Err(error)));
    }

    /// The shipped hub configuration deserializes and obeys its aggregate allocation.
    #[test]
    fn hub_profile_has_finite_aggregate_headroom() -> eyre::Result<()> {
        let network: crate::NetworkConfig =
            serde_json::from_str(include_str!("../../../tools/hub-capacity/profile-v1.json"))?;
        network.gossip_mesh().validate()?;
        network.validate_process_budget(3)?;
        let swarm =
            network.swarm_budget()?.ok_or_else(|| eyre::eyre!("missing hub process budget"))?;
        assert_eq!(swarm.connections(), (64 + 8 + 12 + 2) * 2);
        assert_eq!(swarm.connections_per_peer(), 2);
        assert_eq!(swarm.streams_per_connection(), 8);
        assert_eq!(
            u64::from(swarm.connections()) * 3 * u64::from(swarm.streams_per_connection()),
            4_128
        );
        assert!(
            u64::from(swarm.connections()) * 3 * u64::from(swarm.receive_credit_per_connection())
                <= 1_073_741_824
        );
        assert_eq!(network.peer_config().max_peers(), 86);
        assert_eq!(network.public_peer_limit().map(|limit| limit.get()), Some(64));
        assert_eq!(network.gossip_mesh().target(), 12);
        assert_eq!(network.source_admission().map(|source| source.max_sources()), Some(516));
        Ok(())
    }
}
