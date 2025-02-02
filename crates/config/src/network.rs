//! Configuration for network variables.

use libp2p::{request_response::ProtocolSupport, StreamProtocol};
use std::time::Duration;
use tn_types::{traits::ToFromBytes as _, NetworkPublicKey};

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
    pub max_cert_collection_duration: Duration,
    /// Controls how far ahead of the local node's progress external certificates are allowed to be.
    /// When a certificate arrives from another node, its round number is compared against the highest
    /// round that this node has processed locally. If the difference exceeds this limit, the
    /// certificate is rejected to prevent memory exhaustion from storing too many future certificates.
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
    /// The maximum memory usage would be: 100 * 1000 * 3.3KB = 330MB
    pub max_diff_between_external_cert_round_and_highest_local_round: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_skip_rounds_for_missing_certs: 1_000,
            max_cert_collection_duration: Duration::from_secs(10),
            max_diff_between_external_cert_round_and_highest_local_round: 1_000,
        }
    }
}
