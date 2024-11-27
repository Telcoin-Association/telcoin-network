//! The p2p gossip network for consensus output.
//!
//! Peers subscribe to this network to receive gossip from committee members.

use eyre::eyre;
use futures::StreamExt as _;
use libp2p::{gossipsub, swarm::NetworkBehaviour, SwarmBuilder};
use std::{
    hash::{DefaultHasher, Hash as _, Hasher as _},
    time::Duration,
};
use tn_types::{encode, ConsensusHeader};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info};

/// Network behavior for p2p gossip.
#[derive(NetworkBehaviour)]
struct TelcoinGossip {
    /// Gossipsub
    gossipsub: gossipsub::Behaviour,
}

/// Spawn a gossipsub network for consensus output.
///
/// This network supports clients that follow consensus without participating as a voting committee member.
pub fn start_gossip_publish_network(
    new_output: broadcast::Receiver<ConsensusHeader>,
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
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
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
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )?;

            Ok(TelcoinGossip { gossipsub })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // topic for subscribing
    let topic = gossipsub::IdentTopic::new("tn-consensus");
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;

    // spawn task to broadcast consensus output
    tokio::spawn(async move {
        let mut stream = BroadcastStream::new(new_output);
        while let Some(res) = stream.next().await {
            match res {
                Ok(output) => {
                    let encoded = encode(&output);
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), encoded)
                    {
                        error!(target: "gossip", ?e);
                    }
                }
                Err(e) => {
                    error!(target: "gossip", ?e);
                    break;
                }
            }
        }

        info!(target: "gossip", "gossip publish service shutting down")
    });

    Ok(())
}
