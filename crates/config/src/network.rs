//! Configuration for network variables.

use libp2p::{request_response::ProtocolSupport, StreamProtocol};
use std::time::Duration;
use tn_types::Round;

/// The container for all network configurations.
#[derive(Debug, Default)]
pub struct NetworkConfig {
    /// The configurations for libp2p library.
    ///
    /// This holds parameters for configuring gossipsub and request/response.
    libp2p_config: LibP2pConfig,
    /// The configurations for incoming requests from peers missing certificates.
    sync_config: SyncConfig,
    /// The configurations for quic protocol.
    quic_config: QuicConfig,
    /// The configuration for managing peers.
    peer_config: PeerConfig,
}

impl NetworkConfig {
    /// Return a reference to the [SyncConfig].
    pub fn sync_config(&self) -> &SyncConfig {
        &self.sync_config
    }

    /// Return a reference to the [LibP2pConfig].
    pub fn libp2p_config(&self) -> &LibP2pConfig {
        &self.libp2p_config
    }

    /// Return a reference to the [QuicConfig].
    pub fn quic_config(&self) -> &QuicConfig {
        &self.quic_config
    }

    /// Return a reference to the [PeerConfig].
    pub fn peer_config(&self) -> &PeerConfig {
        &self.peer_config
    }
}

/// Configurations for libp2p library.
#[derive(Debug, Clone)]
pub struct LibP2pConfig {
    /// The supported inbound/outbound protocols for request/response behavior.
    /// - ex) "/telcoin-network/mainnet/0.0.1"
    pub supported_req_res_protocols: Vec<(StreamProtocol, ProtocolSupport)>,
    /// Maximum message size between request/response network messages in bytes.
    pub max_rpc_message_size: usize,
    /// Maximum message size for gossipped messages request/response network messages in bytes.
    ///
    /// The largest gossip message is the `ConsensusHeader`, which influenced the default max.
    ///
    /// Consensus headers are created based on subdag commits which can be several rounds deep.
    /// More benchmarking is needed, but this should be a safe number for a 4-member committee.
    /// - 6 round max between commits
    /// - 4 certificate max per round
    /// - ~300 bytes size per empty certificate
    /// - 5 batch digests max per certificate (32 bytes each)
    /// - (6 * 4)(300 + (5 * 32)) = 11,040
    pub max_gossip_message_size: usize,
}

impl Default for LibP2pConfig {
    fn default() -> Self {
        Self {
            supported_req_res_protocols: vec![(
                StreamProtocol::new("/telcoin-network/0.0.0"),
                ProtocolSupport::Full,
            )],
            max_rpc_message_size: 1024 * 1024, // 1 MiB
            max_gossip_message_size: 12_000,   // 12kb
        }
    }
}

/// Configuration for state syncing operations.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of rounds that can be skipped for a single authority when requesting missing
    /// certificates.
    pub max_skip_rounds_for_missing_certs: usize,
    /// Maximum time to spend collecting certificates from the local storage.
    pub max_db_read_time_for_fetching_certificates: Duration,
    /// Controls how far ahead of the local node's progress external certificates are allowed to
    /// be. When a certificate arrives from another node, its round number is compared against
    /// the highest round that this node has processed locally. If the difference exceeds this
    /// limit, the certificate is rejected to prevent memory exhaustion from storing too many
    /// future certificates.
    ///
    /// For example, if the local node has processed up to round 1000 and this limit is set to 500,
    /// certificates from round 1501 or higher will be rejected. This creates a sliding window of
    /// acceptable rounds that moves forward as the node processes more certificates.
    ///
    /// Memory Impact:
    /// The memory footprint scales with the number of validators (N), this limit (L), and the
    /// certificate size (S). The approximate maximum memory usage is: N * L * S.
    /// With typical values:
    ///   - 100 validators
    ///   - 1000 round limit
    ///   - 3.3KB per certificate
    ///
    /// The maximum memory usage would be: 100 * 1000 * 3.3KB = 330MB
    pub max_diff_between_external_cert_round_and_highest_local_round: u32,
    /// Maximum duration for a round to update before the GC requests certificates from peers.
    ///
    /// On the happy path, this duration should never be reached. It is a safety measure for the
    /// node to try and recover after enough parents weren't received for a round within time.
    pub max_consenus_round_timeout: Duration,
    /// The maximum number of rounds that a proposed header can be behind the node's local round.
    pub max_proposed_header_age_limit: Round,
    /// The tolerable amount of time to wait if a header is proposed before the current time.
    ///
    /// This accounts for small drifts in time keeping between nodes. The timestamp for headers is
    /// currently measured in secs.
    pub max_header_time_drift_tolerance: u64,
    /// The maximum number of missing certificates a CVV peer can request within GC window.
    ///
    /// NOTE: this DOES NOT affect nodes that are syncing full state.
    pub max_num_missing_certs_within_gc_round: usize,
    /// The periodic interval between rounds to directly verify certificates when verifying bulk
    /// sync transfers.
    ///
    /// This value is used by `CertificateValidator::requires_direct_verification`
    pub certificate_verification_round_interval: Round,
    /// The number of certificates to verify within each partitioned chunk.
    ///
    /// This value is used by `CertificateValidator::requires_direct_verification`
    pub certificate_verification_chunk_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_skip_rounds_for_missing_certs: 1_000,
            max_db_read_time_for_fetching_certificates: Duration::from_secs(10),
            max_diff_between_external_cert_round_and_highest_local_round: 1_000,
            max_consenus_round_timeout: Duration::from_secs(30),
            max_proposed_header_age_limit: 3,
            max_header_time_drift_tolerance: 1,
            max_num_missing_certs_within_gc_round: 50,
            certificate_verification_round_interval: 50,
            certificate_verification_chunk_size: 50,
        }
    }
}

/// Configure the quic transport for libp2p.
#[derive(Debug)]
pub struct QuicConfig {
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
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(300),
            max_idle_timeout: 30 * 1000, // 30s
            keep_alive_interval: Duration::from_secs(300),
            max_concurrent_stream_limit: 10_000,
            // may need to increase these based on RTT
            //
            // maximum throughput = (buffer size / round-trip time)
            max_stream_data: 50 * 1024 * 1024,      // 50MiB
            max_connection_data: 100 * 1024 * 1024, // 100MiB
        }
    }
}

// /// The threshold for a peer's score before they are disconnected.
// const MIN_SCORE_BEFORE_DISCONNECT: f64 = -20.0;
// /// The threshold for a peer's score before they are banned.
// const MIN_SCORE_BEFORE_BAN: f64 = -50.0;

/// Configurations for network peers.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// The interval (secs) for updating peer status.
    pub heartbeat_interval: u64,
    /// The target number of peers to maintain connections with.
    pub target_num_peers: usize,
    /// The threshold before a peer is disconnected
    pub min_score_for_disconnect: f64,
    /// The threshold before a peer is banned.
    pub min_score_for_ban: f64,
    /// A fraction of `Self::target_num_peers` that is allowed to connect to this node in excess of
    /// `PeerManager::target_num_peers`.
    ///
    /// NOTE: If `Self::target_num_peers` is 20 and peer_excess_factor = 0.1 this node allows 10% more peers, i.e 22.
    pub peer_excess_factor: f32,

    /// The fraction of extra peers beyond the Self::peer_excess_factor that this node is allowed to dial for
    /// requiring subnet peers.
    ///
    /// NOTE: If the target peer limit is 50, and the excess peer limit is 55, and this node already has 55 peers,
    /// this value provisions a few more slots for dialing priority peers for validator responsibilities.
    pub priority_peer_excess: f32,

    /// A fraction of `Self::target_num_peers` that are outbound-only connections.
    pub target_outbound_only_factor: f32,

    /// A fraction of `Self::target_num_peers` that sets a threshold before the node tries to discovery peers.
    ///
    /// NOTE: Self::min_outbound_only_factor must be < Self::target_outbound_only_factor.
    pub min_outbound_only_factor: f32,

    /// The minimum amount of time before peers are allowed to reconnect after this node disconnects due to too many peers.
    ///
    /// If peers try to connect before the reconnection timeout passes, the swarm denies the connection attempt. This essentially results in a temporary ban at the swarm level.
    pub excess_peers_reconnection_timeout: Duration, // = Duration::from_secs(600);
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: 30,
            target_num_peers: 5,
            min_score_for_disconnect: -20.0,
            min_score_for_ban: -50.0,
            peer_excess_factor: 0.1,
            priority_peer_excess: 0.2,
            target_outbound_only_factor: 0.3,
            min_outbound_only_factor: 0.2,
            excess_peers_reconnection_timeout: Duration::from_secs(600),
        }
    }
}

impl PeerConfig {
    /// The maximum number of peers allowed to connect to this node.
    pub fn max_peers(&self) -> usize {
        (self.target_num_peers as f32 * (1.0 + self.peer_excess_factor)).ceil() as usize
    }

    /// The maximum number of peers allowed when dialing a priority peer.
    ///
    /// Priority peers are known validators that dialed this node or a peer that is explicitly dialed.
    pub fn max_priority_peers(&self) -> usize {
        (self.target_num_peers as f32 * (1.0 + self.peer_excess_factor + self.priority_peer_excess))
            .ceil() as usize
    }

    /// The minimum number of outbound peers that we reach before we start another discovery query.
    pub fn min_outbound_only_peers(&self) -> usize {
        (self.target_num_peers as f32 * self.min_outbound_only_factor).ceil() as usize
    }

    /// The minimum number of outbound peers that we reach before we start another discovery query.
    pub fn target_outbound_peers(&self) -> usize {
        (self.target_num_peers as f32 * self.target_outbound_only_factor).ceil() as usize
    }

    /// The maximum number of peers that are connected or dialing before we refuse to do another
    /// discovery search for more outbound peers. We can use up to half the priority peer excess allocation.
    pub fn max_outbound_dialing_peers(&self) -> usize {
        (self.target_num_peers as f32
            * (1.0 + self.peer_excess_factor + self.priority_peer_excess / 2.0))
            .ceil() as usize
    }
}
