//! The per-role, chain-namespaced wire-protocol names a Telcoin Network peer negotiates on.

use tn_types::WorkerId;

/// The role of a consensus network instance: primary or worker.
///
/// A node runs both as fully isolated libp2p swarms in one process. This is the
/// one source of truth for everything that must differ: the kad store's backing
/// tables and the wire protocol names that keep the two from ever negotiating a
/// connection with one another.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum NetworkType {
    /// Primary network.
    Primary,
    /// Worker network.
    Worker(WorkerId),
}

impl NetworkType {
    /// Request-response wire protocol name, isolated per role (and per worker) and
    /// namespaced by `chain_id` so nodes on different chains never negotiate a
    /// connection.
    ///
    /// Bumped to `/0.0.2` by the #739 legacy-variant deletion: the migration
    /// removed the dead-but-positional request/response variants that were kept
    /// on the wire during the rollout (`StreamEpoch`, `StreamEpochPartial`,
    /// `StreamConsensusOutput`, `MissingCertificates`, the worker
    /// `RequestBatchesStream`, and their acks/replies). Deleting them shifts BCS
    /// variant discriminants, so the protocol version is bumped in the same
    /// change: a `/0.0.2` node never negotiates request-response with a
    /// not-yet-upgraded `/0.0.1` peer, so the two never exchange a stale
    /// discriminant. Only this protocol changed; kad, sync, and peer-exchange
    /// stay at `/0.0.1`.
    pub fn req_res_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-{chain_id}/0.0.2"),
            Self::Worker(id) => format!("/tn-worker-{id}-{chain_id}/0.0.2"),
        }
    }

    /// Kademlia wire protocol name, isolated per role (and per worker) and namespaced
    /// by `chain_id`.
    ///
    /// A client that wants to read this network's node records must negotiate
    /// kad on exactly this name; the primary and each worker are separate DHTs.
    pub fn kad_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-kad-{chain_id}/0.0.1"),
            Self::Worker(id) => format!("/tn-worker-{id}-kad-{chain_id}/0.0.1"),
        }
    }

    /// Bulk-sync streaming wire protocol name, isolated per role (and per worker) and
    /// namespaced by `chain_id` so nodes on different chains never negotiate it.
    pub fn sync_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-sync-{chain_id}/0.0.1"),
            Self::Worker(id) => format!("/tn-worker-{id}-sync-{chain_id}/0.0.1"),
        }
    }

    /// Peer-exchange goodbye wire protocol name, isolated per role (and per worker) and
    /// namespaced by `chain_id` so nodes on different chains never negotiate it.
    pub fn peer_exchange_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-peer-exchange-{chain_id}/0.0.1"),
            Self::Worker(id) => format!("/tn-worker-{id}-peer-exchange-{chain_id}/0.0.1"),
        }
    }
}

/// libp2p gossipsub protocol-id prefix, namespaced by `chain_id` so nodes on
/// different chains can never negotiate a `/meshsub` gossip substream.
///
/// Gossipsub negotiates its own protocol id (libp2p's default `/meshsub/1.1.0`
/// and `/meshsub/1.0.0`), independent of the req-res/kad/stream names above, so
/// without folding the chain id in it is the one wire protocol two chains still
/// share. Feeding this prefix to
/// `gossipsub::ConfigBuilder::protocol_id_prefix`
/// makes the advertised ids `/tn-meshsub-{chain_id}/1.1.0` and
/// `/tn-meshsub-{chain_id}/1.0.0` (the builder appends the `/1.1.0` and `/1.0.0`
/// version suffixes), so cross-chain peers fail multistream-select on gossip the
/// same way they do on the other families.
///
/// The leading `/` is required: `protocol_id_prefix` does not prepend one, and a
/// prefix without it is a malformed `StreamProtocol` that makes
/// `ConfigBuilder::build` fail.
pub fn gossip_protocol_id_prefix(chain_id: u64) -> String {
    format!("/tn-meshsub-{chain_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lock the per-role, chain-namespaced wire-protocol names. These strings are a
    /// peer-compatibility contract: a silent change would prevent peers from
    /// negotiating sessions, and the chain id keeps different chains from ever
    /// negotiating with each other (issue #765).
    #[test]
    fn test_network_type_protocol_names() {
        assert_eq!(NetworkType::Primary.req_res_protocol_name(2017), "/tn-primary-2017/0.0.2");
        assert_eq!(NetworkType::Primary.kad_protocol_name(2017), "/tn-primary-kad-2017/0.0.1");
        assert_eq!(NetworkType::Worker(0).req_res_protocol_name(2017), "/tn-worker-0-2017/0.0.2");
        assert_eq!(NetworkType::Worker(0).kad_protocol_name(2017), "/tn-worker-0-kad-2017/0.0.1");
        // worker id and chain id are both interpolated, not literal
        assert_eq!(NetworkType::Worker(3).req_res_protocol_name(7), "/tn-worker-3-7/0.0.2");
        assert_eq!(NetworkType::Worker(3).kad_protocol_name(7), "/tn-worker-3-kad-7/0.0.1");
        // the per-role sync protocol is chain-namespaced as well
        assert_eq!(NetworkType::Primary.sync_protocol_name(2017), "/tn-primary-sync-2017/0.0.1");
        assert_eq!(NetworkType::Worker(3).sync_protocol_name(7), "/tn-worker-3-sync-7/0.0.1");
        // the per-role peer-exchange goodbye protocol is chain-namespaced as well
        assert_eq!(
            NetworkType::Primary.peer_exchange_protocol_name(2017),
            "/tn-primary-peer-exchange-2017/0.0.1"
        );
        assert_eq!(
            NetworkType::Worker(3).peer_exchange_protocol_name(7),
            "/tn-worker-3-peer-exchange-7/0.0.1"
        );
    }

    /// Every name this crate builds is a well-formed `StreamProtocol` (leading `/`),
    /// so the node-side `StreamProtocol::try_from_owned` wrapper never hits its error path.
    #[test]
    fn test_protocol_names_are_valid_stream_protocols() {
        for nt in [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(7)] {
            for name in [
                nt.req_res_protocol_name(2017),
                nt.kad_protocol_name(2017),
                nt.sync_protocol_name(2017),
                nt.peer_exchange_protocol_name(2017),
            ] {
                assert!(libp2p::StreamProtocol::try_from_owned(name).is_ok());
            }
        }
    }

    /// Lock the chain-namespaced gossipsub protocol-id prefix (issue #765).
    #[test]
    fn test_gossip_protocol_id_prefix_is_chain_namespaced() {
        assert_eq!(gossip_protocol_id_prefix(2017), "/tn-meshsub-2017");
        assert_eq!(gossip_protocol_id_prefix(0), "/tn-meshsub-0");
        assert_ne!(gossip_protocol_id_prefix(1), gossip_protocol_id_prefix(2));
    }
}
