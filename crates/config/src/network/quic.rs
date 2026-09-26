//! QUIC settings shared by the node and independently resolved transport fixtures.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configure the quic transport for libp2p.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct QuicConfig {
    /// Timeout for the initial handshake when establishing a connection.
    /// The actual timeout is the minimum of this and the [`Self::max_idle_timeout`].
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
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(65),
            max_idle_timeout: 30 * 1_000, // 30s
            keep_alive_interval: Duration::from_secs(5),
            max_concurrent_stream_limit: 10_000,
            // may need to increase these based on RTT
            //
            // maximum throughput = (buffer size / round-trip time)
            max_stream_data: 50 * 1024 * 1024,      // 50MiB
            max_connection_data: 100 * 1024 * 1024, // 100MiB
        }
    }
}

impl QuicConfig {
    /// Apply the node's transport settings while preserving libp2p's identity and other defaults.
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
