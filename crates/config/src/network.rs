//! Configuration for network variables.

use libp2p::{request_response::ProtocolSupport, StreamProtocol};
use std::time::Duration;
use tn_types::{traits::ToFromBytes as _, NetworkPublicKey, Round};

/// The container for all network configurations.
#[derive(Debug, Default)]
pub struct NetworkConfig {
    /// The configurations for libp2p library.
    ///
    /// This holds parameters for configuring gossipsub and request/response.
    libp2p_config: LibP2pConfig,
    /// The configurations for incoming requests from peers missing certificates.
    sync_config: SyncConfig,
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

    /// Helper method to convert fastcrypto -> libp2p ed25519.
    pub fn ed25519_fastcrypto_to_libp2p(
        &self,
        fastcrypto: &NetworkPublicKey,
    ) -> Option<libp2p::PeerId> {
        let bytes = fastcrypto.as_ref().to_vec();
        let ed_public_key = libp2p::identity::ed25519::PublicKey::try_from_bytes(&bytes).ok();
        ed_public_key.map(|k| libp2p::PeerId::from_public_key(&k.into()))
    }

    /// Helper method to convert libp2p -> fastcrypto ed25519.
    pub fn ed25519_libp2p_to_fastcrypto(
        &self,
        peer_id: &libp2p::PeerId,
    ) -> Option<fastcrypto::ed25519::Ed25519PublicKey> {
        let bytes = peer_id.as_ref().digest();
        // skip first 4 bytes:
        // - 2 bytes: pubkey type (TN is ed25519 only)
        // - 1 byte: overhead for multihash type
        // - 1 byte: pubkey size
        fastcrypto::ed25519::Ed25519PublicKey::from_bytes(&bytes[4..]).ok()
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
