//! Constants and trait implementations for network compatibility.

use eyre::eyre;
use fastcrypto::hash::Hash as _;
use libp2p::{gossipsub, Multiaddr, Swarm, SwarmBuilder};
use std::time::Duration;
use tn_types::{BlockHash, Certificate, ConsensusHeader, SealedWorkerBlock};
use tracing::error;

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// Convenience trait to make publish network generic over message types.
///
/// The function decodes the `[libp2p::Message]` data field and returns the digest. Using the digest
/// for published message topics makes it easier for peers to recover missing data through the
/// gossip network because the message id is the same as the data type's digest used to reach
/// consensus.
pub trait PublishMessageId<'a>: From<&'a Vec<u8>> {
    /// Create a message id for a published message to the gossip network.
    ///
    /// Lifetimes are preferred for easier maintainability.
    /// ie) encoding/decoding logic is defined in the type's impl of `From`
    fn message_id(msg: &gossipsub::Message) -> BlockHash;
}

// Implementation for worker gossip network.
impl<'a> PublishMessageId<'a> for SealedWorkerBlock {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        // tn_types::decode::<Self>(&msg.data).digest()
        Self::from(&msg.data).digest()
    }
}

// Implementation for primary gossip network.
impl<'a> PublishMessageId<'a> for Certificate {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let certificate = Self::from(&msg.data);
        certificate.digest().into()
    }
}

// Implementation for consensus gossip network.
impl<'a> PublishMessageId<'a> for ConsensusHeader {
    fn message_id(msg: &gossipsub::Message) -> BlockHash {
        let certificate = Self::from(&msg.data);
        certificate.digest().into()
    }
}

/// Generate a swarm type for use with gossip network and start listening.
///
/// This is a convenience function to keep publisher/subscriber network DRY.
///
/// NOTE: the swarm tries to connect to the provided multiaddr.
pub fn start_swarm<'a, M>(multiaddr: Multiaddr) -> eyre::Result<Swarm<gossipsub::Behaviour>>
where
    M: PublishMessageId<'a>,
{
    // generate a random ed25519 key
    let mut swarm = SwarmBuilder::with_new_identity()
        // tokio runtime
        .with_tokio()
        // quic protocol
        .with_quic()
        // custom behavior
        .with_behaviour(|keypair| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let message_id = M::message_id(message);
                gossipsub::MessageId::new(message_id.as_ref())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // this is default - enforce message signing
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be
                // propagated.
                .build()
                .map_err(|e| {
                    error!(?e, "gossipsub publish network");
                    eyre!("failed to build gossipsub config for primary")
                })?;

            // build a gossipsub network behaviour
            let network = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )?;

            Ok(network)
        })
        .expect("worker publish swarm behavior valid")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // start listening
    swarm.listen_on(multiaddr)?;

    Ok(swarm)
}
