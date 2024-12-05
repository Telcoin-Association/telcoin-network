//! Generic abstraction for publishing (flood) to the gossipsub network.

use crate::{
    helpers::{process_network_command, start_swarm},
    types::{
        GossipNetworkHandle, NetworkCommand, PublishMessageId, CONSENSUS_HEADER_TOPIC,
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
    sync::mpsc::{self, Receiver},
    task::JoinHandle,
};
use tracing::{info, trace, warn};

/// The worker's network for publishing sealed worker blocks.
pub struct PublishNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand>,
}

impl PublishNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(
        topic: IdentTopic,
        multiaddr: Multiaddr,
    ) -> eyre::Result<(Self, GossipNetworkHandle)>
    where
        M: PublishMessageId<'a>,
    {
        // create handle
        let (handle_tx, commands) = mpsc::channel(1);
        let handle = GossipNetworkHandle::new(handle_tx);

        // create swarm and start listening
        let swarm = start_swarm::<M>(multiaddr)?;

        // create Self
        let network = Self { topic, network: swarm, commands };

        Ok((network, handle))
    }

    /// Create a new publish network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to publish sealed blocks after they reach quorum.
    pub fn new_for_worker(multiaddr: Multiaddr) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new::<SealedWorkerBlock>(topic, multiaddr)
    }

    /// Create a new publish network for [Certificate].
    ///
    /// This type is used by primary to publish certificates after headers reach quorum.
    pub fn new_for_primary(multiaddr: Multiaddr) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new::<Certificate>(topic, multiaddr)
    }

    /// Create a new publish network for [ConsensusHeader].
    ///
    /// This type is used by consensus to publish consensus block headers after the subdag commits
    /// the latest round (finality).
    pub fn new_for_consensus(multiaddr: Multiaddr) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new::<ConsensusHeader>(topic, multiaddr)
    }

    /// Run the network loop to process incoming gossip.
    pub fn run(mut self) -> JoinHandle<eyre::Result<()>> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = self.network.select_next_some() => self.process_event(event).await?,
                    command = self.commands.recv() => match command {
                        Some(c) => self.process_command(c).await,
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
    async fn process_command(&mut self, command: NetworkCommand) {
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
    use crate::{types::WORKER_BLOCK_TOPIC, SubscriberNetwork};
    use libp2p::{gossipsub::IdentTopic, Multiaddr};
    use std::time::Duration;
    use tn_test_utils::fixture_batch_with_transactions;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    async fn test_publish_to_one_peer() -> eyre::Result<()> {
        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // create publisher
        let (worker_publish_network, worker_publish_network_handle) =
            PublishNetwork::new_for_worker(listen_on.clone())?;

        // create subscriber
        let (tx_sub, mut rx_sub) = mpsc::channel(1);
        let (worker_subscriber_network, worker_subscriber_network_handle) =
            SubscriberNetwork::new_for_worker(tx_sub, listen_on)?;

        // spawn subscriber network
        worker_subscriber_network.run();

        // spawn publish network
        worker_publish_network.run();

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
        let expected_result = Vec::from(&sealed_block);
        let message_id = worker_publish_network_handle
            .publish(IdentTopic::new(WORKER_BLOCK_TOPIC), expected_result.clone())
            .await?;
        let expected_message_id = libp2p::gossipsub::MessageId::new(sealed_block.digest.as_ref());
        assert_eq!(message_id, expected_message_id);

        // wait for subscriber to forward
        let gossip_block = timeout(Duration::from_secs(5), rx_sub.recv())
            .await
            .expect("timeout waiting for gossiped worker block")
            .expect("worker block received");

        assert_eq!(gossip_block, expected_result);

        Ok(())
    }
}
