//! Bounds for incoming QUIC connection attempts.
//!
//! Every swarm (the primary and each worker) owns one QUIC listener endpoint, so every
//! value here is per endpoint. The process-wide bound is `(1 + workers)` times each value.
//!
//! The listener answers an attempt from an address that is not validated with a QUIC Retry
//! (RFC 9000 section 8.1) before it creates connection state. Until the listener decides,
//! quinn holds the attempt in a bounded queue; the values below size that queue from the
//! peer and connection limits of the node.

use libp2p::quic::{Config as QuicTransportConfig, IncomingStats};
use std::sync::Arc;

/// Default largest UDP payload of a quinn endpoint: quinn-proto sets
/// `EndpointConfig::max_udp_payload_size` to 1500 - 28 (Ethernet MTU minus the IP and UDP
/// headers).
const MAX_UDP_PAYLOAD_BYTES: u64 = 1472;

/// Datagrams quinn buffers for one attempt AFTER the first one, before the listener decides
/// on it. quinn does not charge the first datagram of an attempt against
/// `incoming_buffer_size`. The 4 cover the second Initial datagram of a post-quantum hybrid
/// ClientHello, one retransmit of each of the two Initial datagrams, and one spare.
const DATAGRAMS_PER_ATTEMPT: u64 = 4;

/// Incoming attempts per legitimate connection: the first Initial gets a Retry, the second
/// Initial carries the Retry token and gets accepted.
const ATTEMPTS_PER_CONNECTION: usize = 2;

/// Retry, Refuse or Ignore outcomes one listener poll handles before it yields. Equal to the
/// tokio cooperative budget of one task poll.
const OUTCOMES_PER_POLL: usize = 128;

/// Queue bounds for incoming QUIC attempts on one listener endpoint.
///
/// Derivation (defaults: `max_priority_peers` 45, 8 connections per peer):
/// - `L = max_priority_peers * max_connections_per_peer` legitimate concurrent connections (360);
/// - `max_incoming = ATTEMPTS_PER_CONNECTION * L` (720);
/// - `incoming_buffer_size = MAX_UDP_PAYLOAD_BYTES * DATAGRAMS_PER_ATTEMPT` (5888 bytes), for the
///   datagrams after the first one of an attempt;
/// - `incoming_buffer_size_total = max_incoming * incoming_buffer_size` (4_239_360 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuicIncomingLimits {
    /// Maximum attempts queued before the listener decides.
    max_incoming: usize,
    /// Maximum bytes buffered for one queued attempt.
    incoming_buffer_size: u64,
    /// Maximum bytes buffered for all queued attempts.
    incoming_buffer_size_total: u64,
    /// Retry, Refuse or Ignore outcomes per listener poll.
    outcomes_per_poll: usize,
}

impl QuicIncomingLimits {
    /// Derive the bounds from the peer limit and the per-peer connection limit.
    pub(crate) fn new(max_priority_peers: usize, max_connections_per_peer: u32) -> Self {
        let per_peer = usize::try_from(max_connections_per_peer).unwrap_or(usize::MAX);
        let max_incoming = max_priority_peers
            .saturating_mul(per_peer)
            .saturating_mul(ATTEMPTS_PER_CONNECTION)
            .max(ATTEMPTS_PER_CONNECTION);
        let incoming_buffer_size = MAX_UDP_PAYLOAD_BYTES.saturating_mul(DATAGRAMS_PER_ATTEMPT);
        let incoming_buffer_size_total =
            u64::try_from(max_incoming).unwrap_or(u64::MAX).saturating_mul(incoming_buffer_size);
        Self {
            max_incoming,
            incoming_buffer_size,
            incoming_buffer_size_total,
            outcomes_per_poll: OUTCOMES_PER_POLL,
        }
    }

    /// Write the bounds, the Retry switch and the shared counters into a QUIC transport
    /// config. The Retry token lifetime keeps the quinn default.
    pub(crate) fn apply(
        &self,
        config: &mut QuicTransportConfig,
        retry_unvalidated: bool,
        stats: Arc<IncomingStats>,
    ) {
        config.retry_unvalidated_incoming = retry_unvalidated;
        config.max_incoming = Some(self.max_incoming);
        config.incoming_buffer_size = Some(self.incoming_buffer_size);
        config.incoming_buffer_size_total = Some(self.incoming_buffer_size_total);
        config.max_incoming_outcomes_per_poll = self.outcomes_per_poll;
        config.incoming_stats = stats;
    }

    /// Copy of these bounds with a different per-poll outcome cap (tests only).
    #[cfg(test)]
    pub(crate) fn with_outcomes_per_poll(self, outcomes_per_poll: usize) -> Self {
        Self { outcomes_per_poll, ..self }
    }

    /// The bounds as `(max_incoming, incoming_buffer_size, incoming_buffer_size_total)`
    /// (tests only).
    #[cfg(test)]
    pub(crate) fn queue_bounds(&self) -> (usize, u64, u64) {
        (self.max_incoming, self.incoming_buffer_size, self.incoming_buffer_size_total)
    }
}
