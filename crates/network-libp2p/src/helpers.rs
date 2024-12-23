//! Helper methods used for handling network communication.

use crate::{
    codec::TNCodec,
    consensus::TNBehavior,
    types::{NetworkResult, SwarmCommand},
};
use libp2p::{
    gossipsub::{self},
    request_response::{self, Codec, ProtocolSupport},
    Multiaddr, StreamProtocol, Swarm, SwarmBuilder,
};
use std::time::Duration;
use tracing::error;

/// Generate a swarm type for use with gossip network and start listening.
///
/// This is a convenience function to keep publisher/subscriber network DRY.
///
/// NOTE: the swarm tries to connect to the provided multiaddr.
pub(crate) fn start_swarm(
    multiaddr: Multiaddr,
    gossipsub_config: gossipsub::Config,
) -> NetworkResult<Swarm<gossipsub::Behaviour>> {
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
        // TODO: use NetworkError here
        .expect("worker publish swarm behavior valid")
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // start listening
    swarm.listen_on(multiaddr)?;

    Ok(swarm)
}

/// Helper function to apply all specific configurations for all default TN gossipsub behaviors.
///
/// This function ensures that expected defaults are always present, even if the upstream libp2p
/// dependency changes.
fn apply_default_gossipsub_configurations(builder: &mut gossipsub::ConfigBuilder) {
    builder
        // explicitly set heartbeat interval (default)
        .heartbeat_interval(Duration::from_secs(1))
        // explicitly set strict mode (default)
        .validation_mode(gossipsub::ValidationMode::Strict);
}

/// Helper function for publish swarm gossip config.
pub fn subscriber_gossip_config() -> NetworkResult<gossipsub::Config> {
    let mut builder = gossipsub::ConfigBuilder::default();
    apply_default_gossipsub_configurations(&mut builder);
    let config = builder
        // only listen to authorized publishers
        .validate_messages()
        .build()?;

    Ok(config)
}

/// Helper function for publish swarm gossip config.
pub fn publisher_gossip_config() -> NetworkResult<gossipsub::Config> {
    let mut builder = gossipsub::ConfigBuilder::default();
    apply_default_gossipsub_configurations(&mut builder);
    let config = builder
        // support peer exchange
        .do_px()
        .build()?;

    Ok(config)
}

/// Helper function for primary swarm gossip config.
pub fn primary_gossip_config() -> NetworkResult<gossipsub::Config> {
    let mut builder = gossipsub::ConfigBuilder::default();
    apply_default_gossipsub_configurations(&mut builder);
    let config = builder
        // support peer exchange
        .do_px()
        // TODO: probably validate messages here?
        // - depends on the type of message
        //      - match topic
        //      - new consensus data only from current committee
        //      - epoch boundary from all staked validators
        .build()?;

    Ok(config)
}

/// Helper function for processing the network's swarm commands.
///
/// This function calls methods on the swarm.
#[inline]
pub(crate) fn process_swarm_command(
    command: SwarmCommand,
    network: &mut Swarm<gossipsub::Behaviour>,
) {
    match command {
        SwarmCommand::GetListener { reply } => {
            let addrs = network.listeners().cloned().collect();
            if let Err(e) = reply.send(addrs) {
                error!(target: "gossip-network", ?e, "GetListeners command failed");
            }
        }
        SwarmCommand::AddExplicitPeer { peer_id, addr } => {
            network.add_peer_address(peer_id, addr);
            network.behaviour_mut().add_explicit_peer(&peer_id);
        }
        SwarmCommand::Dial { dial_opts, reply } => {
            let res = network.dial(dial_opts);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "AddExplicitPeer command failed");
            }
        }
        SwarmCommand::LocalPeerId { reply } => {
            let peer_id = *network.local_peer_id();
            if let Err(e) = reply.send(peer_id) {
                error!(target: "gossip-network", ?e, "LocalPeerId command failed");
            }
        }
        SwarmCommand::Publish { topic, msg, reply } => {
            let res = network.behaviour_mut().publish(topic, msg);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Publish command failed");
            }
        }
        SwarmCommand::Subscribe { topic, reply } => {
            let res = network.behaviour_mut().subscribe(&topic);
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "Subscribe command failed");
            }
        }
        SwarmCommand::ConnectedPeers { reply } => {
            let res = network.connected_peers().cloned().collect();
            if let Err(e) = reply.send(res) {
                error!(target: "gossip-network", ?e, "ConnectedPeers command failed");
            }
        }
        SwarmCommand::PeerScore { peer_id, reply } => {
            let opt_score = network.behaviour_mut().peer_score(&peer_id);
            if let Err(e) = reply.send(opt_score) {
                error!(target: "gossip-network", ?e, "PeerScore command failed");
            }
        }
        SwarmCommand::SetApplicationScore { peer_id, new_score, reply } => {
            let bool = network.behaviour_mut().set_application_score(&peer_id, new_score);
            if let Err(e) = reply.send(bool) {
                error!(target: "gossip-network", ?e, "SetApplicationScore command failed");
            }
        }
        SwarmCommand::AllPeers { reply } => {
            let collection = network
                .behaviour_mut()
                .all_peers()
                .map(|(peer_id, vec)| (*peer_id, vec.into_iter().cloned().collect()))
                .collect();

            if let Err(e) = reply.send(collection) {
                error!(target: "gossip-network", ?e, "AllPeers command failed");
            }
        }
        SwarmCommand::AllMeshPeers { reply } => {
            let collection = network.behaviour_mut().all_mesh_peers().cloned().collect();
            if let Err(e) = reply.send(collection) {
                error!(target: "gossip-network", ?e, "AllMeshPeers command failed");
            }
        }
        SwarmCommand::MeshPeers { topic, reply } => {
            let collection = network.behaviour_mut().mesh_peers(&topic).cloned().collect();
            if let Err(e) = reply.send(collection) {
                error!(target: "gossip-network", ?e, "MeshPeers command failed");
            }
        }
    }
}
