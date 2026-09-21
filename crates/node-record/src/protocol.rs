//! Chain-scoped wire protocol names.

use tn_types::WorkerId;

/// The role of a consensus network instance: primary or worker.
///
/// A node runs both as fully isolated libp2p swarms in one process. This is the
/// one source of truth for everything that must differ: the kad store's backing
/// tables and the wire protocol names that keep the two from ever negotiating a
/// connection with one another.
#[derive(Copy, Clone, Debug)]
pub enum NetworkType {
    /// Primary network.
    Primary,
    /// Worker network.
    Worker(WorkerId),
}

impl NetworkType {
    /// Request-response wire protocol, isolated per role (and per worker) and
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

    /// Kademlia wire protocol, isolated per role (and per worker) and namespaced
    /// by `chain_id`.
    pub fn kad_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-kad-{chain_id}/0.0.1"),
            Self::Worker(id) => format!("/tn-worker-{id}-kad-{chain_id}/0.0.1"),
        }
    }

    /// Bulk-sync streaming wire protocol, isolated per role (and per worker) and
    /// namespaced by `chain_id` so nodes on different chains never negotiate it.
    ///
    /// The stream behaviour registers this as its sole upgrade; the typed
    /// `SyncFrame` layer rides on streams negotiated
    /// with this protocol.
    pub fn sync_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-sync-{chain_id}/0.0.1"),
            Self::Worker(id) => format!("/tn-worker-{id}-sync-{chain_id}/0.0.1"),
        }
    }

    /// Peer-exchange goodbye wire protocol, isolated per role (and per worker) and
    /// namespaced by `chain_id` so nodes on different chains never negotiate it.
    ///
    /// A dedicated request-response protocol for the `PeerExchangeMap`
    /// a node shares when it gracefully disconnects. Goodbyes prefer this protocol and fall
    /// back to the variant embedded in the consensus request enums when the peer has not
    /// upgraded yet (`UnsupportedProtocols` is penalty-exempt).
    pub fn peer_exchange_protocol_name(&self, chain_id: u64) -> String {
        match self {
            Self::Primary => format!("/tn-primary-peer-exchange-{chain_id}/0.0.1"),
            Self::Worker(id) => {
                format!("/tn-worker-{id}-peer-exchange-{chain_id}/0.0.1")
            }
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
