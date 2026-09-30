//! Errors a [`KadClient`](crate::KadClient) reports.

use libp2p::Multiaddr;

/// Hint appended to connectivity errors so a wrong-role or wrong-port bootstrap address is
/// diagnosable from the message alone.
const ROLE_PORT_HINT: &str = "the primary DHT listens on udp/49590 and the worker DHT on \
                              udp/49594 by default; check --primary/--worker-id and the bootstrap \
                              addresses";

/// Every way a lookup through the client can fail.
///
/// "Not found" is not an error: [`KadClient::get_node_record`](crate::KadClient::get_node_record)
/// returns `Ok(None)` when the DHT answered and holds no record for the key. Each variant's
/// message is written to be printed verbatim by a CLI.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KadClientError {
    /// A bootstrap address is unusable: it has no trailing `/p2p/<peer-id>` component or that
    /// component does not parse.
    #[error("invalid bootstrap address {addr}: {reason}")]
    InvalidBootstrapAddr {
        /// The offending address.
        addr: Multiaddr,
        /// Why it was rejected.
        reason: String,
    },
    /// The config carried no bootstrap addresses, so there is nothing to dial.
    #[error("no bootstrap peers configured: at least one /ip4/../udp/../quic-v1/p2p/<peer-id> address is required")]
    NoBootstrapPeers,
    /// Every bootstrap peer was dialed and none connected before the deadline.
    #[error("no bootstrap peer reachable: every dial failed or timed out; {ROLE_PORT_HINT}")]
    NoBootstrapPeerReachable,
    /// The lookup contacted peers but none of them would speak the client's kademlia protocol.
    ///
    /// The transport connected, so the address and port are live, but the remote runs a different
    /// DHT: most likely the bootstrap address points at the primary swarm while the client was
    /// configured for a worker (or the reverse), or the chain ids differ.
    #[error("no peer answered on kademlia protocol {protocol}: {requests} request(s) were sent and every one failed, so the bootstrap peers are on a different DHT (role or chain); {ROLE_PORT_HINT}")]
    NoPeerAnswered {
        /// The protocol name the client negotiated on.
        protocol: String,
        /// How many requests the lookup issued before giving up.
        requests: u32,
    },
    /// The lookup ran past the configured deadline without producing a valid record.
    #[error("kademlia lookup timed out before any valid record was returned")]
    Timeout,
    /// The DHT returned one or more copies of the record but every copy failed verification.
    ///
    /// Distinct from `Ok(None)`: peers do hold a record under this key, but none of them was
    /// signed for the client's `(chain_id, network_type)` domain by the requested BLS key with a
    /// publisher matching the record's network identity. Either the client is configured for the
    /// wrong chain or role, the publisher runs node software that predates domain-scoped record
    /// signing (GHSA-cc64-wfq5-56ph) and so signs no domain at all, or the stored record is
    /// poisoned. The pre-domain case is deliberately not accepted: doing so would reopen the
    /// cross-role replay the domain closed.
    #[error("{copies} record copy/copies were returned but none verified: the record is signed for a different chain or role than this client is configured for, was published by node software that predates domain-scoped record signing (GHSA-cc64-wfq5-56ph), or it is poisoned")]
    InvalidRecords {
        /// How many record copies the DHT returned for the key.
        copies: usize,
    },
    /// The libp2p swarm could not be built or the transport could not start.
    #[error("transport error: {0}")]
    Transport(String),
    /// The background driver task has exited, so the client can no longer issue queries.
    #[error("kademlia client is shut down")]
    Shutdown,
}
