//! QUIC settings shared by the node and independently resolved transport fixtures.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configure the quic transport for libp2p.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct QuicConfig {
    /// Timeout for the initial handshake when establishing a connection.
    /// The actual timeout is the minimum of this and the [`Self::max_idle_timeout`].
    /// Each signed endpoint gets this window when dialing addresses in order.
    pub handshake_timeout: Duration,
    /// Maximum duration of inactivity in ms to accept before timing out the connection.
    pub max_idle_timeout: u32,
    /// Period of inactivity before sending a keep-alive packet.
    /// Must be set lower than the idle_timeout of both
    /// peers to be effective.
    ///
    /// This configures quinn's transport keep-alive interval.
    pub keep_alive_interval: Duration,
    /// Maximum number of incoming bidirectional streams that may be open
    /// concurrently by the remote peer.
    pub max_concurrent_stream_limit: u32,
    /// Max unacknowledged data in bytes that may be sent on a single stream.
    pub max_stream_data: u32,
    /// Max unacknowledged data in bytes that may be sent in total on all streams
    /// of a connection.
    pub max_connection_data: u32,
    /// Answer every incoming QUIC connection attempt whose source address is not
    /// validated with a QUIC Retry packet (RFC 9000 section 8.1) before the listener
    /// creates connection state. The remote must echo the token from its address.
    ///
    /// Default `true`. Set `false` only as an operator rollback switch.
    pub retry_unvalidated_incoming: bool,
}

/// Default ceiling on concurrent incoming bidirectional streams per connection.
///
/// This is the libp2p QUIC default. A peer opens one stream per request, gossip frame or sync
/// session, and the serve budgets hold far fewer than this in flight, so the value is headroom for
/// a well-behaved peer and a bound for a hostile one. quinn buffers data for every opened stream
/// until the swarm accepts it in index order, and every empty stream ahead of the data costs one
/// inbound upgrade timeout, so the stream ceiling also bounds how long buffered data can wait.
pub const DEFAULT_MAX_CONCURRENT_STREAM_LIMIT: u32 = 256;

/// Default receive credit per stream, in bytes: 2 MiB.
///
/// A request-response message is at most `max_rpc_message_size` (1 MiB by default), a gossip frame
/// stays under 2 MiB, and a sync frame is read in bounded chunks, so one stream window holds the
/// largest message a protocol reads in one step with the same amount of headroom. A larger message
/// still arrives: quinn re-issues stream credit as the handler reads.
pub const DEFAULT_MAX_STREAM_DATA: u32 = 2 * 1024 * 1024;

/// Default receive credit per connection, in bytes: 4 MiB, two full stream windows.
///
/// This is the most data one connection can make the node buffer before the application reads it.
/// With the per-peer and per-swarm connection ceilings of the consensus network, a free peer id
/// pins at most 32 MiB and a whole swarm at most 512 MiB. The previous 100 MiB window allowed
/// 800 MiB per peer id and had no swarm bound. Throughput per connection stays at the window over
/// the round-trip time: 40 MiB/s at 100 ms.
pub const DEFAULT_MAX_CONNECTION_DATA: u32 = 4 * 1024 * 1024;

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            max_idle_timeout: 30 * 1_000, // 30s
            keep_alive_interval: Duration::from_secs(5),
            max_concurrent_stream_limit: DEFAULT_MAX_CONCURRENT_STREAM_LIMIT,
            max_stream_data: DEFAULT_MAX_STREAM_DATA,
            max_connection_data: DEFAULT_MAX_CONNECTION_DATA,
            retry_unvalidated_incoming: true,
        }
    }
}

impl QuicConfig {
    /// Apply the shared transport settings while preserving libp2p's identity and other defaults.
    ///
    /// The node applies its incoming connection policy separately because the release fixtures
    /// use transports without that policy API.
    pub fn apply_to(&self, mut config: libp2p::quic::Config) -> libp2p::quic::Config {
        config.handshake_timeout = self.handshake_timeout;
        config.max_idle_timeout = self.max_idle_timeout;
        config.keep_alive_interval = self.keep_alive_interval;
        config.max_concurrent_stream_limit = self.max_concurrent_stream_limit;
        config.max_stream_data = self.max_stream_data;
        config.max_connection_data = self.max_connection_data;
        config
    }
}

#[cfg(test)]
mod tests {
    use super::QuicConfig;
    use std::time::Duration;

    /// Every configurable field must reach the transport, including nondefault operator settings.
    #[test]
    fn applies_all_transport_settings() {
        let settings = QuicConfig {
            handshake_timeout: Duration::from_secs(17),
            max_idle_timeout: 23_000,
            keep_alive_interval: Duration::from_secs(3),
            max_concurrent_stream_limit: 19,
            max_stream_data: 12_345,
            max_connection_data: 54_321,
            ..QuicConfig::default()
        };
        let key = libp2p::identity::Keypair::generate_ed25519();
        let config = settings.apply_to(libp2p::quic::Config::new(&key));
        assert_eq!(config.handshake_timeout, settings.handshake_timeout);
        assert_eq!(config.max_idle_timeout, settings.max_idle_timeout);
        assert_eq!(config.keep_alive_interval, settings.keep_alive_interval);
        assert_eq!(config.max_concurrent_stream_limit, settings.max_concurrent_stream_limit);
        assert_eq!(config.max_stream_data, settings.max_stream_data);
        assert_eq!(config.max_connection_data, settings.max_connection_data);
    }
}
