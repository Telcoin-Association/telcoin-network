//! Configuration for network variables.

use crate::{ConfigFmt, ConfigTrait, TelcoinDirs};
use libp2p::kad::K_VALUE;
use serde::{Deserialize, Serialize};
use std::{num::NonZeroUsize, time::Duration};
use tn_types::Round;

impl ConfigTrait for NetworkConfig {}

/// The container for all network configurations.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(default)]
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
    /// The hostname for the validator.
    hostname: String,
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

    /// Return a mutable reference to the [LibP2pConfig].
    #[cfg(feature = "test-utils")]
    pub fn libp2p_config_mut(&mut self) -> &mut LibP2pConfig {
        &mut self.libp2p_config
    }

    /// Return a reference to the [QuicConfig].
    pub fn quic_config(&self) -> &QuicConfig {
        &self.quic_config
    }

    /// Return a reference to the [PeerConfig].
    pub fn peer_config(&self) -> &PeerConfig {
        &self.peer_config
    }

    /// The human-readable identity for this node.
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    /// Return a mutable reference to the [PeerConfig].
    #[cfg(feature = "test-utils")]
    pub fn peer_config_mut(&mut self) -> &mut PeerConfig {
        &mut self.peer_config
    }

    /// Read a network config file.
    pub fn read_config<TND: TelcoinDirs>(tn_datadir: &TND) -> eyre::Result<Self> {
        let path = tn_datadir.network_config_path();
        Self::load_from_path_or_default(path, ConfigFmt::YAML)
    }

    /// Write the current network config to file.
    pub fn write_config<TND: TelcoinDirs>(&self, tn_datadir: &TND) -> eyre::Result<()> {
        let path = tn_datadir.network_config_path();
        Self::write_to_path(path, self, ConfigFmt::YAML)
    }
}

/// Configurations for libp2p library.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct LibP2pConfig {
    /// Maximum message size between request/response network messages in bytes.
    pub max_rpc_message_size: usize,
    /// Maximum message size for gossipped network messages in bytes.
    ///
    /// The largest gossip message is likely a `Certificate`.
    /// The default of 12,000 bytes should be enough for legit gossip.
    /// Large messages should not be gossipped
    pub max_gossip_message_size: usize,
    /// The maximum duration to keep an idle connection alive between peers.
    ///
    /// The strategy for TN is to rely on QUIC to send keep alive messages instead of adding
    /// another behaviour to the swarm.
    pub max_idle_connection_timeout: Duration,
    /// The maximum number of pending peer-exchance disconnect messages before this node
    /// immediately disconnects.
    ///
    /// When Self::target_num_peers is reached, this node disconnects gracefully from the connected
    /// peer and exchanges peer information to faciliate discovery. The current solution
    /// requires an async task to wait for an ack before timing out. This is a workaround until
    /// a custom RPC NetworkBehaviour is implemented. The ideal approach is to use a custom
    /// `ConnectionHandler` and support an event that disconnects as soon as the message is
    /// sent.
    ///
    /// This limit is set to prevent a node from spawning too many disconnect tasks.
    pub max_px_disconnects: usize,
    /// The maximum amount of time to wait for a peer's ack during a px_disconnect before forcing
    /// the disconnect.
    pub px_disconnect_timeout: Duration,
    /// The k-bucket size for kademlia.
    pub k_bucket_size: NonZeroUsize,
    /// The TTL applied to kademlia records — both the libp2p record TTL and the
    /// local store's `expires` timestamp. Drives eviction of records that are
    /// never refreshed. Also used for provider record TTL.
    pub kad_record_ttl: Duration,
    /// How often this node republishes its own kademlia records.
    ///
    /// Must be < `kad_record_ttl`, otherwise records expire before they are
    /// refreshed.
    pub kad_publication_interval: Duration,
}

impl LibP2pConfig {
    /// Return topics for primary.
    pub fn primary_topic() -> String {
        String::from("tn-primary")
    }

    /// Return topics for primary.
    pub fn consensus_output_topic() -> String {
        String::from("tn-consensus-output")
    }

    /// Return topics for epoch votes.
    pub fn epoch_vote_topic() -> String {
        String::from("tn-epoch-vote")
    }

    /// Return topics for worker.
    pub fn worker_batch_topic() -> String {
        String::from("tn-worker")
    }

    /// Return topics for worker.
    pub fn worker_txn_topic() -> String {
        String::from("tn-txn")
    }
}

impl Default for LibP2pConfig {
    fn default() -> Self {
        Self {
            max_rpc_message_size: 1024 * 1024,                    // 1 MiB
            max_gossip_message_size: 12_000,                      // 12kb
            max_idle_connection_timeout: Duration::from_secs(65), // same as quic handshake
            max_px_disconnects: 10,
            px_disconnect_timeout: Duration::from_secs(3),
            k_bucket_size: K_VALUE,
            kad_record_ttl: Duration::from_secs(48 * 60 * 60),
            kad_publication_interval: Duration::from_secs(12 * 60 * 60),
        }
    }
}

/// Configuration for state syncing operations.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
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
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
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

/// Configurations for network peers.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
#[serde(default)]
pub struct PeerConfig {
    /// The interval (secs) for updating peer status.
    pub heartbeat_interval: u64,
    /// The target number of connected peers.
    pub target_num_peers: usize,
    /// The timeout for dialing a peer.
    pub dial_timeout: Duration,
    /// The threshold before a peer is disconnected
    pub min_score_for_disconnect: f64,
    /// The threshold before a peer is banned.
    pub min_score_for_ban: f64,
    /// A fraction of `Self::target_num_peers` that is allowed to connect to this node in excess of
    /// `PeerManager::target_num_peers`.
    ///
    /// NOTE: If `Self::target_num_peers` is 20 and peer_excess_factor = 0.1 this node allows 10%
    /// more peers, i.e 22.
    pub peer_excess_factor: f32,
    /// The fraction of extra peers beyond the Self::peer_excess_factor that this node is allowed
    /// to dial for requiring subnet peers.
    ///
    /// NOTE: If the target peer limit is 50, and the excess peer limit is 55, and this node
    /// already has 55 peers, this value provisions a few more slots for dialing priority peers
    /// for validator responsibilities.
    pub priority_peer_excess: f32,
    /// A fraction of `Self::target_num_peers` that are outbound-only connections.
    pub target_outbound_only_factor: f32,
    /// A fraction of `Self::target_num_peers` that sets a threshold before the node tries to
    /// discovery peers.
    ///
    /// NOTE: Self::min_outbound_only_factor must be < Self::target_outbound_only_factor.
    pub min_outbound_only_factor: f32,
    /// The minimum amount of time before peers are allowed to reconnect after this node
    /// disconnects due to too many peers.
    ///
    /// If peers try to connect before the reconnection timeout passes, the swarm denies the
    /// connection attempt. This essentially results in a temporary ban at the swarm level.
    pub excess_peers_reconnection_timeout: Duration,
    /// The maximum number of banned peers to maintain before pruning.
    pub max_banned_peers: usize,
    /// The maximum number of disconnected peers to maintain before pruning.
    pub max_disconnected_peers: usize,
    /// The config for scoring peers.
    pub score_config: ScoreConfig,
}

impl Default for PeerConfig {
    fn default() -> Self {
        // 50% more than kademlia target
        let target_num_peers = (K_VALUE.get() / 2) + K_VALUE.get();

        Self {
            heartbeat_interval: 30,
            target_num_peers,
            dial_timeout: Duration::from_secs(15),
            min_score_for_disconnect: -20.0,
            min_score_for_ban: -50.0,
            peer_excess_factor: 0.3,
            priority_peer_excess: 0.2,
            target_outbound_only_factor: 0.3,
            min_outbound_only_factor: 0.2,
            excess_peers_reconnection_timeout: Duration::from_secs(600),
            max_banned_peers: 100,
            max_disconnected_peers: 100,
            score_config: ScoreConfig::default(),
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
    /// Priority peers are known validators that dialed this node or a peer that is explicitly
    /// dialed.
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
    /// discovery search for more outbound peers. We can use up to half the priority peer excess
    /// allocation.
    pub fn max_outbound_dialing_peers(&self) -> usize {
        (self.target_num_peers as f32
            * (1.0 + self.peer_excess_factor + self.priority_peer_excess / 2.0))
            .ceil() as usize
    }

    /// The max number of peers to maintain for potential discovery attempts.
    pub fn max_discovery_peers(&self) -> usize {
        self.target_num_peers * 2
    }
}

/// Configuration for peer scoring parameters
#[derive(Serialize, Deserialize, Clone, Debug, Copy)]
#[serde(default)]
pub struct ScoreConfig {
    /// The default score for new peers.
    pub default_score: f64,
    /// The threshold for a peer's score before they are banned, regardless of any other scoring
    /// parameters.
    pub min_application_score_before_ban: f64,
    /// The maximum score a peer can obtain.
    pub max_score: f64,
    /// The minimum score a peer can obtain.
    pub min_score: f64,
    /// The halflife of a peer's score in seconds.
    pub score_halflife: f64,
    /// The minimum amount of time (seconds) a peer is banned before their score begins to decay.
    pub banned_before_decay_secs: u64,
    /// Minimum score before a peer is disconnected.
    pub min_score_before_disconnect: f64,
    /// Minimum score before a peer is banned.
    pub min_score_before_ban: f64,
}

impl Default for ScoreConfig {
    fn default() -> Self {
        ScoreConfig {
            default_score: 0.0,
            min_application_score_before_ban: -60.0,
            max_score: 100.0,
            min_score: -100.0,
            // 5-minute halflife: transient WAN penalties (peer flap, slow request) decay
            // out within a couple of half-lives so they do not accumulate across the
            // observer-join window.
            score_halflife: 300.0,
            // Short lockout so a peer that crosses the ban threshold can recover within
            // one operator-attention window. DoS protection comes from the ban threshold
            // itself, not from the lockout duration — a peer that keeps misbehaving will
            // simply hit the threshold again.
            banned_before_decay_secs: 30 * 60, // 30 minutes
            min_score_before_disconnect: -20.0,
            min_score_before_ban: -50.0,
        }
    }
}

impl ScoreConfig {
    /// Returns the banned before decay duration
    pub fn banned_before_decay(&self) -> Duration {
        Duration::from_secs(self.banned_before_decay_secs)
    }

    /// Calculate the halflife decay constant =
    /// -(2.0f64.ln()) / self.score_halflife
    pub fn halflife_decay(&self) -> f64 {
        -(2.0f64.ln()) / self.score_halflife
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yaml_deserializes_to_default() {
        let parsed: NetworkConfig = serde_yaml::from_str("{}").expect("empty mapping deserializes");
        let default = NetworkConfig::default();

        assert_eq!(parsed.hostname, default.hostname);
        assert_eq!(parsed.libp2p_config.kad_record_ttl, default.libp2p_config.kad_record_ttl);
        assert_eq!(
            parsed.libp2p_config.kad_publication_interval,
            default.libp2p_config.kad_publication_interval
        );
        assert_eq!(parsed.peer_config.target_num_peers, default.peer_config.target_num_peers);
        assert_eq!(
            parsed.sync_config.max_skip_rounds_for_missing_certs,
            default.sync_config.max_skip_rounds_for_missing_certs
        );
        assert_eq!(parsed.quic_config.max_idle_timeout, default.quic_config.max_idle_timeout);
    }

    #[test]
    fn partial_yaml_uses_defaults_for_missing_fields() {
        // libp2p_config provided with only a subset of fields; the two new kad
        // fields (and others) are absent and must fall back to defaults.
        let yaml = r#"
libp2p_config:
  max_rpc_message_size: 2097152
  max_gossip_message_size: 12000
  max_idle_connection_timeout:
    secs: 65
    nanos: 0
  max_px_disconnects: 10
  px_disconnect_timeout:
    secs: 3
    nanos: 0
  k_bucket_size: 20
hostname: "my-validator"
"#;
        let parsed: NetworkConfig =
            serde_yaml::from_str(yaml).expect("partial config deserializes");
        let default = LibP2pConfig::default();

        // explicitly-set field is preserved
        assert_eq!(parsed.libp2p_config.max_rpc_message_size, 2 * 1024 * 1024);
        assert_eq!(parsed.hostname, "my-validator");

        // missing new fields fall back to defaults
        assert_eq!(parsed.libp2p_config.kad_record_ttl, default.kad_record_ttl);
        assert_eq!(parsed.libp2p_config.kad_publication_interval, default.kad_publication_interval);

        // entirely-missing sub-sections also default
        assert_eq!(parsed.peer_config.target_num_peers, PeerConfig::default().target_num_peers);
    }

    #[test]
    fn legacy_libp2p_config_without_kad_fields_deserializes() {
        let default = LibP2pConfig::default();
        let serialized = serde_yaml::to_string(&default).expect("serialize default");

        // Round-trip through serde_yaml::Value so we can drop the two new
        // kad fields cleanly without depending on their serialized layout
        // (Duration serializes as a multi-line mapping).
        let mut value: serde_yaml::Value =
            serde_yaml::from_str(&serialized).expect("serialized default re-parses");
        let mapping = value.as_mapping_mut().expect("libp2p config is a mapping");
        assert!(
            mapping.remove(&serde_yaml::Value::String("kad_record_ttl".into())).is_some(),
            "kad_record_ttl must be present in the default serialization"
        );
        assert!(
            mapping.remove(&serde_yaml::Value::String("kad_publication_interval".into())).is_some(),
            "kad_publication_interval must be present in the default serialization"
        );
        let legacy = serde_yaml::to_string(&value).expect("serialize stripped value");

        let parsed: LibP2pConfig =
            serde_yaml::from_str(&legacy).expect("legacy yaml without kad fields parses");

        assert_eq!(parsed.kad_record_ttl, default.kad_record_ttl);
        assert_eq!(parsed.kad_publication_interval, default.kad_publication_interval);
        assert_eq!(parsed.max_rpc_message_size, default.max_rpc_message_size);
        assert_eq!(parsed.k_bucket_size, default.k_bucket_size);
    }
}
