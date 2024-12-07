//! Helper methods used for handling network communication.

use crate::types::{GossipNetworkMessage, NetworkCommand};
use eyre::eyre;
use libp2p::{
    gossipsub::{self},
    identity::Keypair,
    Multiaddr, Swarm, SwarmBuilder,
};
use std::time::Duration;
use tracing::error;

/// Generate a swarm type for use with gossip network and start listening.
///
/// This is a convenience function to keep publisher/subscriber network DRY.
///
/// NOTE: the swarm tries to connect to the provided multiaddr.
pub(crate) fn start_swarm<M>(
    multiaddr: Multiaddr,
    gossipsub_config: gossipsub::Config,
) -> eyre::Result<Swarm<gossipsub::Behaviour>>
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

/// Helper function for publish swarm gossip config.
pub(crate) fn subscriber_gossip_config() -> eyre::Result<gossipsub::Config> {
    let config = gossipsub::ConfigBuilder::default()
        // explicitly set heartbeat interval (default)
        .heartbeat_interval(Duration::from_secs(1))
        // explicitly set strict mode (default)
        .validation_mode(gossipsub::ValidationMode::Strict)
        // only listen to authorized publishers
        .validate_messages()
        .build()?;

    Ok(config)
}

/// Helper function for publish swarm gossip config.
pub(crate) fn publisher_gossip_config() -> eyre::Result<gossipsub::Config> {
    let config = gossipsub::ConfigBuilder::default()
        // explicitly set heartbeat interval (default)
        .heartbeat_interval(Duration::from_secs(1))
        // explicitly set strict mode (default)
        .validation_mode(gossipsub::ValidationMode::Strict)
        // support peer exchange
        .do_px()
        .build()?;

    Ok(config)
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
        NetworkCommand::SetApplicationScore { peer_id, new_score, reply } => {
            let bool = network.behaviour_mut().set_application_score(&peer_id, new_score);
            if let Err(e) = reply.send(bool) {
                error!(target: "gossip-network", ?e, "SetApplicationScore command failed");
            }
        }
    }
}
