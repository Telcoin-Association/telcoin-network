//! Configuration for a [`KadClient`](crate::KadClient).

use libp2p::Multiaddr;
use std::time::Duration;
use tn_node_record::{NetworkType, RecordDomain};

/// Default per-query deadline.
///
/// Generous enough for a QUIC handshake plus an iterative lookup across a handful of peers, short
/// enough that a CLI lookup against a dead bootstrap address fails in a reasonable time.
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(15);

/// Everything a [`KadClient`](crate::KadClient) needs to reach one Telcoin Network DHT.
///
/// The `(chain_id, network_type)` pair selects a single DHT: the primary swarm and each worker
/// swarm are separate kademlia networks with separate protocol names, and a record's signature is
/// bound to exactly one of them (see [`RecordDomain`]). The bootstrap addresses must belong to
/// nodes of that same DHT, or the client will connect at the transport layer and then find no
/// peer willing to speak its kademlia protocol.
#[derive(Clone, Debug)]
pub struct KadClientConfig {
    /// The chain whose DHT to read. Folded into the kademlia protocol name and the record
    /// signing domain.
    pub chain_id: u64,
    /// Which of the node's swarms to read: the primary DHT or a specific worker's DHT.
    ///
    /// Only worker records carry [`RpcInfo`](tn_node_record::RpcInfo); the node publishes its
    /// primary record with `rpc: None`.
    pub network_type: NetworkType,
    /// Bootstrap peers to seed the routing table with and dial at startup.
    ///
    /// Each address MUST end in `/p2p/<peer-id>`: kademlia keys its routing table by peer id, and
    /// the QUIC transport uses the id to authenticate the remote. [`KadClient::spawn`] rejects any
    /// address without one.
    ///
    /// [`KadClient::spawn`]: crate::KadClient::spawn
    pub bootstrap: Vec<Multiaddr>,
    /// Per-query deadline.
    ///
    /// Applied as the kademlia query timeout and enforced independently by the client so a lookup
    /// against an unresponsive peer resolves even if the DHT layer never reports progress. Also
    /// bounds how long [`KadClient::spawn`] waits for the first bootstrap connection.
    ///
    /// [`KadClient::spawn`]: crate::KadClient::spawn
    pub query_timeout: Duration,
}

impl KadClientConfig {
    /// Build a config with the [`DEFAULT_QUERY_TIMEOUT`].
    pub fn new(chain_id: u64, network_type: NetworkType, bootstrap: Vec<Multiaddr>) -> Self {
        Self { chain_id, network_type, bootstrap, query_timeout: DEFAULT_QUERY_TIMEOUT }
    }

    /// Override the per-query deadline.
    pub fn with_query_timeout(mut self, query_timeout: Duration) -> Self {
        self.query_timeout = query_timeout;
        self
    }

    /// The `(chain, role)` domain every record fetched through this config must be signed for.
    pub fn record_domain(&self) -> RecordDomain {
        RecordDomain::new(self.chain_id, self.network_type)
    }
}
