//! Generic abstraction for publishing (flood) to the gossipsub network.
//!
//! This network impl is intended to send consensus data using floodsub.
//!
//! This network does not subscribe to the topics it publishes.

use crate::{
    helpers::{process_network_command, publisher_gossip_config, start_swarm},
    types::{
        GossipNetworkHandle, GossipNetworkMessage, NetworkCommand, CONSENSUS_HEADER_TOPIC,
        PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
    },
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{self, IdentTopic},
    swarm::SwarmEvent,
    Multiaddr, Swarm,
};
use tn_types::{Certificate, ConsensusHeader, SealedWorkerBlock};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::{info, trace, warn};

/// The worker's network for publishing sealed worker blocks.
pub struct PublishNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The sender for network handles.
    handle: Sender<NetworkCommand>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand>,
}

impl PublishNetwork {
    /// Create a new instance of Self.
    pub fn new<M>(
        topic: IdentTopic,
        multiaddr: Multiaddr,
        gossipsub_config: gossipsub::Config,
    ) -> eyre::Result<Self>
    where
        M: GossipNetworkMessage,
    {
        // create handle
        let (handle, commands) = mpsc::channel(1);

        // create swarm and start listening
        let swarm = start_swarm::<M>(multiaddr, gossipsub_config)?;

        // create Self
        let network = Self { topic, network: swarm, handle, commands };

        Ok(network)
    }

    /// Return a [GossipNetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> GossipNetworkHandle {
        GossipNetworkHandle::new(self.handle.clone())
    }

    /// Create a new publish network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to publish sealed blocks after they reach quorum.
    pub fn new_for_worker(
        multiaddr: Multiaddr,
        gossipsub_config: gossipsub::Config,
    ) -> eyre::Result<Self> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new::<SealedWorkerBlock>(topic, multiaddr, gossipsub_config)
    }

    /// Create a new publish network for [Certificate].
    ///
    /// This type is used by primary to publish certificates after headers reach quorum.
    pub fn new_for_primary(
        multiaddr: Multiaddr,
        gossipsub_config: gossipsub::Config,
    ) -> eyre::Result<Self> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new::<Certificate>(topic, multiaddr, gossipsub_config)
    }

    /// Create a new publish network for [ConsensusHeader].
    ///
    /// This type is used by consensus to publish consensus block headers after the subdag commits
    /// the latest round (finality).
    pub fn new_for_consensus(
        multiaddr: Multiaddr,
        gossipsub_config: gossipsub::Config,
    ) -> eyre::Result<Self> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new::<ConsensusHeader>(topic, multiaddr, gossipsub_config)
    }

    /// Run the network loop to process incoming gossip.
    pub fn run(mut self) -> JoinHandle<eyre::Result<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = self.network.select_next_some() => self.process_event(event).await?,
                    command = self.commands.recv() => match command {
                        Some(c) => self.process_command(c),
                        None => {
                            info!(target: "subscriber-network", topic=?self.topic, "subscriber shutting down...");
                            return Ok(())
                        }
                    }
                }
            }
        })
    }

    /// Process commands for the swarm.
    fn process_command(&mut self, command: NetworkCommand) {
        process_network_command(command, &mut self.network);
    }

    /// Process events from the swarm.
    async fn process_event(&mut self, event: SwarmEvent<gossipsub::Event>) -> eyre::Result<()> {
        match event {
            SwarmEvent::Behaviour(gossip) => match gossip {
                gossipsub::Event::Message { propagation_source, message_id, message } => {
                    warn!(target: "publish-network", topic=?self.topic, ?propagation_source, ?message_id, ?message, "unexpected gossip message received!")
                }
                gossipsub::Event::Subscribed { peer_id, topic } => {
                    trace!(target: "publish-network", topic=?self.topic, ?peer_id, ?topic, "gossipsub event - subscribed")
                }
                gossipsub::Event::Unsubscribed { peer_id, topic } => {
                    trace!(target: "publish-network", topic=?self.topic, ?peer_id, ?topic, "gossipsub event - unsubscribed")
                }
                gossipsub::Event::GossipsubNotSupported { peer_id } => {
                    // TODO: remove peer at self point?
                    trace!(target: "publish-network", topic=?self.topic, ?peer_id, "gossipsub event - not supported")
                }
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                concurrent_dial_errors,
                established_in,
            } => {
                trace!(target: "publish-network", topic=?self.topic, ?peer_id, ?connection_id, ?endpoint, ?num_established, ?concurrent_dial_errors, ?established_in, "connection established")
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                cause,
            } => trace!(
                target: "publish-network",
                topic=?self.topic,
                ?peer_id,
                ?connection_id,
                ?endpoint,
                ?num_established,
                ?cause,
                "connection closed"
            ),
            SwarmEvent::IncomingConnection { connection_id, local_addr, send_back_addr } => {
                trace!(target: "publish-network", topic=?self.topic, ?connection_id, ?local_addr, ?send_back_addr, "incoming connection")
            }
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                error,
            } => trace!(
                target: "publish-network",
                topic=?self.topic,
                ?connection_id,
                ?local_addr,
                ?send_back_addr,
                ?error,
                "incoming connection error"
            ),
            SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error } => {
                trace!(target: "publish-network", topic=?self.topic, ?connection_id, ?peer_id, ?error, "outgoing connection error")
            }
            SwarmEvent::NewListenAddr { listener_id, address } => {
                trace!(target: "publish-network", topic=?self.topic, ?listener_id, ?address, "new listener addr")
            }
            SwarmEvent::ExpiredListenAddr { listener_id, address } => {
                trace!(target: "publish-network", topic=?self.topic, ?listener_id, ?address, "expired listen addr")
            }
            SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                trace!(target: "publish-network", topic=?self.topic, ?listener_id, ?addresses, ?reason, "listener closed")
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                trace!(target: "publish-network", topic=?self.topic, ?listener_id, ?error, "listener error")
            }
            SwarmEvent::Dialing { peer_id, connection_id } => {
                trace!(target: "publish-network", topic=?self.topic, ? peer_id, ?connection_id, "dialing")
            }
            SwarmEvent::NewExternalAddrCandidate { address } => {
                trace!(target: "publish-network", topic=?self.topic, ?address, "new external addr candidate")
            }
            SwarmEvent::ExternalAddrConfirmed { address } => {
                trace!(target: "publish-network", topic=?self.topic, ?address, "external addr confirmed")
            }
            SwarmEvent::ExternalAddrExpired { address } => {
                trace!(target: "publish-network", topic=?self.topic, ?address, "external addr expired")
            }
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                trace!(target: "publish-network", topic=?self.topic, ?peer_id, ?address, "new external addr of peer")
            }
            _e => {
                trace!(target: "publish-network", topic=?self.topic, ?_e, "non-exhaustive event match")
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PublishNetwork;
    use crate::{
        helpers::{publisher_gossip_config, subscriber_gossip_config},
        types::WORKER_BLOCK_TOPIC,
        SubscriberNetwork,
    };
    use libp2p::{gossipsub::IdentTopic, Multiaddr, PeerId};
    use std::{collections::HashSet, str::FromStr as _, time::Duration};
    use tn_test_utils::fixture_batch_with_transactions;
    use tn_types::SealedWorkerBlock;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    async fn test_publish_to_one_peer() -> eyre::Result<()> {
        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // create publisher
        let default_pub_config = publisher_gossip_config()?;
        let worker_publish_network =
            PublishNetwork::new_for_worker(listen_on.clone(), default_pub_config)?;
        let worker_publish_network_handle = worker_publish_network.network_handle();

        // spawn publish network
        worker_publish_network.run();

        // obtain publisher's peer id
        let cvv = worker_publish_network_handle.local_peer_id().await?;

        // create subscriber
        let (tx_sub, mut rx_sub) = mpsc::channel::<SealedWorkerBlock>(1);
        let default_sub_config = subscriber_gossip_config()?;
        let worker_subscriber_network = SubscriberNetwork::new_for_worker(
            tx_sub,
            listen_on,
            HashSet::from([cvv]),
            default_sub_config,
        )?;
        let worker_subscriber_network_handle = worker_subscriber_network.network_handle();

        // spawn subscriber network
        worker_subscriber_network.run();

        // yield for network to start so listeners update
        tokio::task::yield_now().await;

        let pub_listeners = worker_publish_network_handle.listeners().await?;
        let pub_addr = pub_listeners.first().expect("pub network is listening").clone();

        // dial publisher to exchange information
        worker_subscriber_network_handle.dial(pub_addr.into()).await?;

        // allow enough time for peer info to exchange from dial
        //
        // sleep seems to be the only thing that works here
        tokio::time::sleep(Duration::from_secs(1)).await;

        // publish random block
        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::try_from(sealed_block.clone())?;
        let _message_id = worker_publish_network_handle
            .publish(IdentTopic::new(WORKER_BLOCK_TOPIC), expected_result.clone())
            .await?;

        // wait for subscriber to forward
        let gossip_block = timeout(Duration::from_secs(5), rx_sub.recv())
            .await
            .expect("timeout waiting for gossiped worker block")
            .expect("worker block received");

        assert_eq!(gossip_block, sealed_block);

        Ok(())
    }
}
