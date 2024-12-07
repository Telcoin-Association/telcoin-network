//! Helper methods used for handling network communication.

use crate::types::{GossipNetworkMessage, NetworkCommand};
use eyre::eyre;
use libp2p::{
    gossipsub::{self},
    Multiaddr, Swarm, SwarmBuilder,
};
use std::time::Duration;
use tracing::error;

/// Generate a swarm type for use with gossip network and start listening.
///
/// This is a convenience function to keep publisher/subscriber network DRY.
///
/// NOTE: the swarm tries to connect to the provided multiaddr.
pub(crate) fn start_swarm<M>(multiaddr: Multiaddr) -> eyre::Result<Swarm<gossipsub::Behaviour>>
where
    M: GossipNetworkMessage,
{
    // generate a random ed25519 key
    let mut swarm = SwarmBuilder::with_new_identity()
        // tokio runtime
        .with_tokio()
        // quic protocol
        .with_quic()
        // custom behavior
        .with_behaviour(|keypair| {
            // configure peer score parameters
            //
            // default for now
            let score_params = gossipsub::PeerScoreParams::default();

            // configure thresholds at which peers are considered faulty or malicious
            //
            // peer baseline is 0
            let score_thresholds = gossipsub::PeerScoreThresholds {
                gossip_threshold: -10.0,   // ignore gossip to and from peer
                publish_threshold: -20.0,  // don't flood publish to this peer
                graylist_threshold: -50.0, // effectively ignore peer
                accept_px_threshold: 10.0, // score only attainable by validators
                opportunistic_graft_threshold: 5.0,
            };

            // set a custom gossipsub configuration
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                // valid messages must decode to the expected message types
                .validate_messages()
                // explicitly set strict mode (default)
                .validation_mode(gossipsub::ValidationMode::Strict)
                // .peer_score_params(score_params)
                // .peer_score_thresholds(score_thresholds)
                .build()
                .map_err(|e| {
                    error!(?e, "gossipsub publish network");
                    eyre!("failed to build gossipsub config for primary")
                })?;

            // build a gossipsub network behaviour
            let mut network = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                gossipsub_config,
            )?;

            // enable peer scoring
            network.with_peer_score(score_params, score_thresholds)?;

            Ok(network)
        })
        .expect("worker publish swarm behavior valid")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // start listening
    swarm.listen_on(multiaddr)?;

    Ok(swarm)
}

/// Helper function for processing network commands.
///
/// This function calls methods on the swarm.
#[inline]
pub(crate) fn process_network_command(
    command: NetworkCommand,
    network: &mut Swarm<gossipsub::Behaviour>,
) {
    match command {
        NetworkCommand::GetListener { reply } => {
            let addrs = network.listeners().cloned().collect();
            if let Err(e) = reply.send(addrs) {
                error!(target: "gossip-network", ?e, "GetListeners command failed");
            }
        }
        NetworkCommand::AddExplicitPeer { peer_id, addr } => {
            network.add_peer_address(peer_id, addr);
            network.behaviour_mut().add_explicit_peer(&peer_id);
        }
        NetworkCommand::Dial { dial_opts, reply } => {
            let res = network.dial(dial_opts);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "AddExplicitPeer command failed");
            }
        }
        NetworkCommand::LocalPeerId { reply } => {
            let peer_id = *network.local_peer_id();
            if let Err(e) = reply.send(peer_id) {
                error!(target: "gossip-network", ?e, "LocalPeerId command failed");
            }
        }
        NetworkCommand::Publish { topic, msg, reply } => {
            let res = network.behaviour_mut().publish(topic, msg);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Publish command failed");
            }
        }
        NetworkCommand::Subscribe { topic, reply } => {
            let res = network.behaviour_mut().subscribe(&topic);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Subscribe command failed");
            }
        }
        NetworkCommand::ConnectedPeers { reply } => {
            let res = network.connected_peers().cloned().collect();
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "ConnectedPeers command failed");
            }
        }
        NetworkCommand::PeerScore { peer_id, reply } => {
            let opt_score = network.behaviour_mut().peer_score(&peer_id);
            if let Err(e) = reply.send(opt_score) {
                error!(target: "gossip-network", ?e, "PeerScore command failed");
            }
        }
    }
}
