//! Worker gossipsub network.

use eyre::eyre;
use libp2p::{gossipsub, swarm::NetworkBehaviour, SwarmBuilder};
use std::time::Duration;
use tn_types::{encode, WorkerBlock};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{error, info};

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";

/// The worker's gossipsub network.
#[derive(NetworkBehaviour)]
pub struct WorkerGossip {
    /// The handle to the network.
    ///
    /// Use this to publish messages.
    network: gossipsub::Behaviour,
}

/// Spawn a gossipsub network for worker sealed blocks.
///
/// This network publishes (flood) worker blocks that reach quorum.
/// Subscribers propogate the message to other peers through network gossip.
///
/// The worker's keypair is used to sign messages.
/// The worker block's digest is used as the message id.
///
/// The network swarm uses QUIC - default port 30304.
pub fn start_worker_gossip_network(
    new_worker_block: mpsc::Receiver<WorkerBlock>,
) -> eyre::Result<()> {
    // generate a random ed25519 key
    let mut swarm = SwarmBuilder::with_new_identity()
        // tokio runtime
        .with_tokio()
        // quic protocol
        .with_quic()
        // custom behavior
        .with_behaviour(|keypair| {
            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |_message: &gossipsub::Message| {
                // TODO: ssz decode message data to worker block
                // and use digest
                let s = "todo!";
                gossipsub::MessageId::from(s.to_string())
            };

            // Set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // this is default - enforce message signing
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
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

            Ok(WorkerGossip { network })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // worker topic for publishing sealed blocks
    let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    // spawn task to broadcast consensus output
    tokio::spawn(async move {
        let mut stream = ReceiverStream::new(new_worker_block);
        while let Some(worker_block) = stream.next().await {
            let encoded = encode(&worker_block);
            if let Err(e) = swarm.behaviour_mut().network.publish(topic.clone(), encoded) {
                error!(target: "worker::gossip::publish", ?e);
            }
        }

        info!(target: "worker::gossip::publish", "gossip publish service shutting down")
    });

    Ok(())
}
