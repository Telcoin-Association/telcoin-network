//! QUIC settings shared by the node and independently resolved transport fixtures.

use serde::{Deserialize, Serialize};
use std::{num::NonZeroUsize, time::Duration};

/// Where a QUIC listener decides on inbound connection attempts and accepts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum InboundAcceptTask {
    /// The shipped default: the listener decides and accepts inside the swarm task.
    #[default]
    Inline,
    /// A measurement prototype for issue #1741: a dedicated task decides and accepts, and
    /// the accepted handshakes wait in a bounded queue until the swarm takes them.
    Spawned {
        /// Accepted handshakes that may wait for the swarm at one time.
        capacity: NonZeroUsize,
    },
}

impl InboundAcceptTask {
    /// The accepted-handshake queue capacity of the spawned mode; `None` keeps the inline path.
    ///
    /// The node writes this to the carried transport. The release fixtures have no such field.
    pub const fn spawned_capacity(self) -> Option<NonZeroUsize> {
        match self {
            Self::Inline => None,
            Self::Spawned { capacity } => Some(capacity),
        }
    }
}

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
    /// Where a listener decides on inbound connection attempts and accepts them.
    ///
    /// Default [`InboundAcceptTask::Inline`]. The spawned mode is a measurement prototype
    /// for issue #1741 and not a production setting.
    pub inbound_accept_task: InboundAcceptTask,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            max_idle_timeout: 30 * 1_000, // 30s
            keep_alive_interval: Duration::from_secs(5),
            max_concurrent_stream_limit: 10_000,
            // may need to increase these based on RTT
            //
            // maximum throughput = (buffer size / round-trip time)
            max_stream_data: 50 * 1024 * 1024,      // 50MiB
            max_connection_data: 100 * 1024 * 1024, // 100MiB
            retry_unvalidated_incoming: true,
            inbound_accept_task: InboundAcceptTask::Inline,
        }
    }
}

impl QuicConfig {
    /// Apply the shared transport settings while preserving libp2p's identity and other defaults.
    ///
    /// The node applies its incoming connection policy and its inbound accept task separately
    /// because the release fixtures use transports without those APIs.
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
    use super::{InboundAcceptTask, QuicConfig};
    use std::{num::NonZeroUsize, time::Duration};

    /// A nondefault accept queue capacity (64).
    const CAPACITY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(63);

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

    /// The shipped default keeps the inline path, which maps to no spawned capacity, and the
    /// spawned mode maps to its capacity.
    #[test]
    fn inline_default_keeps_the_inline_path() {
        assert_eq!(QuicConfig::default().inbound_accept_task, InboundAcceptTask::Inline);
        assert_eq!(InboundAcceptTask::Inline.spawned_capacity(), None);
        let spawned = InboundAcceptTask::Spawned { capacity: CAPACITY };
        assert_eq!(spawned.spawned_capacity(), Some(CAPACITY));
    }

    /// The accept task setting round-trips through serde, an explicit spawned section parses,
    /// and an empty section yields the inline default.
    #[test]
    fn inbound_accept_task_round_trips() -> Result<(), serde_json::Error> {
        let spawned = QuicConfig {
            inbound_accept_task: InboundAcceptTask::Spawned { capacity: CAPACITY },
            ..QuicConfig::default()
        };
        let text = serde_json::to_string(&spawned)?;
        assert!(text.contains(r#""mode":"spawned""#), "{text}");
        assert!(text.contains(r#""capacity":64"#), "{text}");
        let parsed: QuicConfig = serde_json::from_str(&text)?;
        assert_eq!(parsed.inbound_accept_task, spawned.inbound_accept_task);
        let explicit: QuicConfig =
            serde_json::from_str(r#"{"inbound_accept_task":{"mode":"spawned","capacity":64}}"#)?;
        assert_eq!(explicit.inbound_accept_task, InboundAcceptTask::Spawned { capacity: CAPACITY });
        let empty: QuicConfig = serde_json::from_str("{}")?;
        assert_eq!(empty.inbound_accept_task, InboundAcceptTask::Inline);
        Ok(())
    }
}
