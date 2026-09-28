// Copyright 2017-2020 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{sync::Arc, time::Duration};

use quinn::{
    MtuDiscoveryConfig, VarInt,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};

use crate::incoming::IncomingStats;

/// Config for the transport.
#[derive(Clone)]
pub struct Config {
    /// Timeout for the initial handshake when establishing a connection.
    /// The actual timeout is the minimum of this and the [`Config::max_idle_timeout`].
    pub handshake_timeout: Duration,
    /// Maximum duration of inactivity in ms to accept before timing out the connection.
    pub max_idle_timeout: u32,
    /// Period of inactivity before sending a keep-alive packet.
    /// Must be set lower than the idle_timeout of both
    /// peers to be effective.
    ///
    /// See [`quinn::TransportConfig::keep_alive_interval`] for more
    /// info.
    pub keep_alive_interval: Duration,
    /// Maximum number of incoming bidirectional streams that may be open
    /// concurrently by the remote peer.
    pub max_concurrent_stream_limit: u32,

    /// Max unacknowledged data in bytes that may be sent on a single stream.
    pub max_stream_data: u32,

    /// Max unacknowledged data in bytes that may be sent in total on all streams
    /// of a connection.
    pub max_connection_data: u32,

    /// Support QUIC version draft-29 for dialing and listening.
    ///
    /// Per default only QUIC Version 1 / [`libp2p_core::multiaddr::Protocol::QuicV1`]
    /// is supported.
    ///
    /// If support for draft-29 is enabled servers support draft-29 and version 1 on all
    /// QUIC listening addresses.
    /// As client the version is chosen based on the remote's address.
    #[deprecated(note = "QUIC draft versions are no longer supported")]
    pub support_draft_29: bool,

    /// TLS client config for the inner [`quinn::ClientConfig`].
    client_tls_config: Arc<QuicClientConfig>,
    /// TLS server config for the inner [`quinn::ServerConfig`].
    server_tls_config: Arc<QuicServerConfig>,
    /// Libp2p identity of the node.
    keypair: libp2p_identity::Keypair,

    /// Parameters governing MTU discovery. See [`MtuDiscoveryConfig`] for details.
    mtu_discovery_config: Option<MtuDiscoveryConfig>,

    /// Answer every incoming connection attempt whose source address is not
    /// validated with a QUIC Retry packet (RFC 9000 section 8.1), before the
    /// listener creates any connection state for it. The remote must echo the
    /// Retry token from its address to get a handshake.
    ///
    /// Default `false` (upstream behavior: accept every attempt).
    pub retry_unvalidated_incoming: bool,

    /// Lifetime of the Retry tokens this endpoint issues.
    /// `None` keeps the quinn default. See [`quinn::ServerConfig::retry_token_lifetime`].
    pub retry_token_lifetime: Option<Duration>,

    /// Maximum number of incoming attempts one endpoint holds before the
    /// listener decides on them. Further Initial packets are dropped.
    /// `None` keeps the quinn default. See [`quinn::ServerConfig::max_incoming`].
    pub max_incoming: Option<usize>,

    /// Maximum bytes buffered for one incoming attempt before the listener
    /// decides on it. `None` keeps the quinn default.
    /// See [`quinn::ServerConfig::incoming_buffer_size`].
    pub incoming_buffer_size: Option<u64>,

    /// Maximum bytes buffered for all incoming attempts of one endpoint.
    /// `None` keeps the quinn default.
    /// See [`quinn::ServerConfig::incoming_buffer_size_total`].
    pub incoming_buffer_size_total: Option<u64>,

    /// Maximum number of Retry, Refuse or Ignore outcomes a listener handles in
    /// one poll. When a listener reaches it, the listener wakes its task and
    /// returns `Poll::Pending`, so that the task yields to the executor.
    /// An Accept outcome always returns an event at once. Values below 1 act as 1.
    pub max_incoming_outcomes_per_poll: usize,

    /// Shared counters of the listener decisions. Keep a clone of the [`Arc`]
    /// before the transport is built to read them.
    pub incoming_stats: Arc<IncomingStats>,
}

#[expect(deprecated)]
impl Config {
    /// Creates a new configuration object with default values.
    pub fn new(keypair: &libp2p_identity::Keypair) -> Self {
        let client_tls_config = Arc::new(
            QuicClientConfig::try_from(libp2p_tls::make_client_config(keypair, None).unwrap())
                .unwrap(),
        );
        let server_tls_config = Arc::new(
            QuicServerConfig::try_from(libp2p_tls::make_server_config(keypair).unwrap()).unwrap(),
        );
        Self {
            client_tls_config,
            server_tls_config,
            support_draft_29: false,
            handshake_timeout: Duration::from_secs(5),
            max_idle_timeout: 10 * 1000,
            max_concurrent_stream_limit: 256,
            keep_alive_interval: Duration::from_secs(5),
            max_connection_data: 15_000_000,

            // Ensure that one stream is not consuming the whole connection.
            max_stream_data: 10_000_000,
            keypair: keypair.clone(),
            mtu_discovery_config: Some(Default::default()),
            retry_unvalidated_incoming: false,
            retry_token_lifetime: None,
            max_incoming: None,
            incoming_buffer_size: None,
            incoming_buffer_size_total: None,
            max_incoming_outcomes_per_poll: 128,
            incoming_stats: Arc::new(IncomingStats::default()),
        }
    }

    /// Set the upper bound to the max UDP payload size that MTU discovery will search for.
    pub fn mtu_upper_bound(mut self, value: u16) -> Self {
        self.mtu_discovery_config
            .get_or_insert_with(Default::default)
            .upper_bound(value);
        self
    }

    /// Disable MTU path discovery (it is enabled by default).
    pub fn disable_path_mtu_discovery(mut self) -> Self {
        self.mtu_discovery_config = None;
        self
    }
}

/// Represents the inner configuration for [`quinn`].
#[derive(Debug, Clone)]
pub(crate) struct QuinnConfig {
    pub(crate) client_config: quinn::ClientConfig,
    pub(crate) server_config: quinn::ServerConfig,
    pub(crate) endpoint_config: quinn::EndpointConfig,
}

#[expect(deprecated)]
impl From<Config> for QuinnConfig {
    fn from(config: Config) -> QuinnConfig {
        let Config {
            client_tls_config,
            server_tls_config,
            max_idle_timeout,
            max_concurrent_stream_limit,
            keep_alive_interval,
            max_connection_data,
            max_stream_data,
            support_draft_29,
            handshake_timeout: _,
            keypair,
            mtu_discovery_config,
            retry_unvalidated_incoming: _,
            retry_token_lifetime,
            max_incoming,
            incoming_buffer_size,
            incoming_buffer_size_total,
            max_incoming_outcomes_per_poll: _,
            incoming_stats: _,
        } = config;
        let mut transport = quinn::TransportConfig::default();
        // Disable uni-directional streams.
        transport.max_concurrent_uni_streams(0u32.into());
        transport.max_concurrent_bidi_streams(max_concurrent_stream_limit.into());
        // Disable datagrams.
        transport.datagram_receive_buffer_size(None);
        transport.keep_alive_interval(Some(keep_alive_interval));
        transport.max_idle_timeout(Some(VarInt::from_u32(max_idle_timeout).into()));
        transport.allow_spin(false);
        transport.stream_receive_window(max_stream_data.into());
        transport.receive_window(max_connection_data.into());
        transport.mtu_discovery_config(mtu_discovery_config);
        let transport = Arc::new(transport);

        let mut server_config = quinn::ServerConfig::with_crypto(server_tls_config);
        server_config.transport = Arc::clone(&transport);
        // Disables connection migration.
        // Long-term this should be enabled, however we then need to handle address change
        // on connections in the `Connection`.
        server_config.migration(false);
        if let Some(lifetime) = retry_token_lifetime {
            server_config.retry_token_lifetime(lifetime);
        }
        if let Some(max_incoming) = max_incoming {
            server_config.max_incoming(max_incoming);
        }
        if let Some(size) = incoming_buffer_size {
            server_config.incoming_buffer_size(size);
        }
        if let Some(size) = incoming_buffer_size_total {
            server_config.incoming_buffer_size_total(size);
        }

        let mut client_config = quinn::ClientConfig::new(client_tls_config);
        client_config.transport_config(transport);

        let mut endpoint_config = keypair
            .derive_secret(b"libp2p quic stateless reset key")
            .map(|secret| {
                let reset_key = Arc::new(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &secret));
                quinn::EndpointConfig::new(reset_key)
            })
            .unwrap_or_default();

        if !support_draft_29 {
            endpoint_config.supported_versions(vec![1]);
        }

        QuinnConfig {
            client_config,
            server_config,
            endpoint_config,
        }
    }
}
