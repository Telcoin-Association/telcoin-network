//! Generic abstraction for publishing (flood) to the gossipsub network.

use crate::types::{
    process_network_command, start_swarm, GossipNetworkHandle, NetworkCommand, PublishMessageId,
    CONSENSUS_HEADER_TOPIC, PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
};
use futures::{ready, StreamExt as _};
use libp2p::{
    gossipsub::{self, IdentTopic},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tn_types::{Certificate, ConsensusHeader, SealedWorkerBlock};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, trace, warn};

/// The worker's network for publishing sealed worker blocks.
pub struct PublishNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for receiving sealed worker blocks to publish.
    stream: ReceiverStream<Vec<u8>>,
    /// The receiver for processing network handle requests.
    commands: ReceiverStream<NetworkCommand>,
    // /// The [Multiaddr] for the swarm.
    // multiaddr: Multiaddr,
}

impl PublishNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(
        topic: IdentTopic,
        receiver: mpsc::Receiver<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<(Self, GossipNetworkHandle)>
    where
        M: PublishMessageId<'a>,
    {
        // create handle
        let (handle_tx, network_rx) = mpsc::channel(1);
        let commands = ReceiverStream::new(network_rx);
        let handle = GossipNetworkHandle::new(handle_tx);

        // create swarm and start listening
        let swarm = start_swarm::<M>(multiaddr)?;

        // convert receiver into stream for convenience in `Self::poll`
        let stream = ReceiverStream::new(receiver);

        // create Self
        let network = Self { topic, network: swarm, stream, commands };

        Ok((network, handle))
    }

    /// Return this publisher's [PeerId].
    pub fn local_peer_id(&self) -> &PeerId {
        self.network.local_peer_id()
    }

    /// Return an collection of addresses the network is listening on.
    ///
    /// NOTE: until the swarm events are polled, this will return empty.
    /// See unit tests for implementation.
    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.network.listeners().cloned().collect()
    }

    /// Add an explicit peer to support further discovery.
    pub fn add_explicit_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.network.behaviour_mut().add_explicit_peer(&peer_id);
        self.network.add_peer_address(peer_id, addr);
    }

    /// Spawn the network to start publishing gossip.
    ///
    /// Calls [`Swarm::listen_on`] and spawns `Self` as a future.
    pub fn spawn(self) -> JoinHandle<()> {
        // spawn future
        tokio::task::spawn(self)
    }

    /// Create a new publish network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to publish sealed blocks after they reach quorum.
    pub fn new_for_worker(
        receiver: mpsc::Receiver<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new::<SealedWorkerBlock>(topic, receiver, multiaddr)
    }

    /// Create a new publish network for [Certificate].
    ///
    /// This type is used by primary to publish certificates after headers reach quorum.
    pub fn new_for_primary(
        receiver: mpsc::Receiver<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new::<Certificate>(topic, receiver, multiaddr)
    }

    /// Create a new publish network for [ConsensusHeader].
    ///
    /// This type is used by consensus to publish consensus block headers after the subdag commits the latest round (finality).
    pub fn new_for_consensus(
        receiver: mpsc::Receiver<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new::<ConsensusHeader>(topic, receiver, multiaddr)
    }
}

impl Future for PublishNetwork {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        info!(target: "publish-network", topic=?this.topic, "starting poll...");

        // handle network commands first
        //
        // this allows handles to be dropped without causing problems
        if let Poll::Ready(Some(command)) = this.commands.poll_next_unpin(cx) {
            process_network_command(command, &mut this.network);
        }

        // process swarm events
        loop {
            match this.network.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => {
                    debug!(target: "publish-network", ?event, "Swarm event occurred");
                    // Process swarm events here, such as handling new connections or errors
                    match event {
                        SwarmEvent::Behaviour(gossip) => match gossip {
                            gossipsub::Event::Message {
                                propagation_source,
                                message_id,
                                message,
                            } => {
                                warn!(target: "publish-network", topic=?this.topic, ?propagation_source, ?message_id, ?message, "unexpected gossip message received!")
                            }
                            gossipsub::Event::Subscribed { peer_id, topic } => {
                                trace!(target: "publish-network", topic=?this.topic, ?peer_id, ?topic)
                            }
                            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                                trace!(target: "publish-network", topic=?this.topic, ?peer_id, ?topic)
                            }
                            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                                // TODO: remove peer at this point?
                                trace!(target: "publish-network", topic=?this.topic, ?peer_id)
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
                            trace!(target: "publish-network", topic=?this.topic, ?peer_id, ?connection_id, ?endpoint, ?num_established, ?concurrent_dial_errors, ?established_in)
                        }
                        SwarmEvent::ConnectionClosed {
                            peer_id,
                            connection_id,
                            endpoint,
                            num_established,
                            cause,
                        } => trace!(
                            target: "publish-network",
                            topic=?this.topic,
                            ?peer_id,
                            ?connection_id,
                            ?endpoint,
                            ?num_established,
                            ?cause,
                        ),
                        SwarmEvent::IncomingConnection {
                            connection_id,
                            local_addr,
                            send_back_addr,
                        } => {
                            trace!(target: "publish-network", topic=?this.topic, ?connection_id, ?local_addr, ?send_back_addr)
                        }
                        SwarmEvent::IncomingConnectionError {
                            connection_id,
                            local_addr,
                            send_back_addr,
                            error,
                        } => trace!(
                            target: "publish-network",
                            topic=?this.topic,
                            ?connection_id,
                            ?local_addr,
                            ?send_back_addr,
                            ?error,
                        ),
                        SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error } => {
                            trace!(target: "publish-network", topic=?this.topic, ?connection_id, ?peer_id, ?error,)
                        }
                        SwarmEvent::NewListenAddr { listener_id, address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?listener_id, ?address)
                        }
                        SwarmEvent::ExpiredListenAddr { listener_id, address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?listener_id, ?address)
                        }
                        SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                            trace!(target: "publish-network", topic=?this.topic, ?listener_id, ?addresses, ?reason)
                        }
                        SwarmEvent::ListenerError { listener_id, error } => {
                            trace!(target: "publish-network", topic=?this.topic, ?listener_id, ?error)
                        }
                        SwarmEvent::Dialing { peer_id, connection_id } => {
                            trace!(target: "publish-network", topic=?this.topic, ? peer_id, ?connection_id)
                        }
                        SwarmEvent::NewExternalAddrCandidate { address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?address)
                        }
                        SwarmEvent::ExternalAddrConfirmed { address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?address)
                        }
                        SwarmEvent::ExternalAddrExpired { address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?address)
                        }
                        SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                            trace!(target: "publish-network", topic=?this.topic, ?peer_id, ?address)
                        }
                        _e => {
                            trace!(target: "publish-network", topic=?this.topic, ?_e, "non-exhaustive event match")
                        }
                    }
                }
                Poll::Pending => break,
                Poll::Ready(None) => return Poll::Ready(()), // swarm closed
            }
        }

        // publish messages to the network after swarm updates
        while let Some(msg) = ready!(this.stream.poll_next_unpin(cx)) {
            debug!(target: "publish-network", topic=?this.topic, "publishing msg :D");
            if let Err(e) = this.network.behaviour_mut().publish(this.topic.clone(), msg) {
                error!(target: "publish-network", ?e);
                break;
            }
        }

        info!(target: "publish-network", topic=?this.topic, "publisher shutting down...");
        Poll::Ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::PublishNetwork;
    use crate::SubscriberNetwork;
    use futures::StreamExt as _;
    use libp2p::{swarm::SwarmEvent, Multiaddr};
    use std::time::Duration;
    use tn_types::WorkerBlock;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    async fn test_generics_compile() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();

        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // spawn publish
        let (tx_pub, rx_pub) = mpsc::channel(1);
        let (mut worker_publish_network, worker_publish_network_handle) =
            PublishNetwork::new_for_worker(rx_pub, listen_on.clone())?;

        // spawn subscriber
        let (tx_sub, mut rx_sub) = mpsc::channel(1);
        let (mut worker_subscriber_network, worker_subscriber_network_handle) =
            SubscriberNetwork::new_for_worker(tx_sub, listen_on)?;

        // dial publisher to establish connection
        //
        // NOTE: this doesn't work - add peer/addr and then dial by peer_id
        // worker_subscriber_network.add_explicit_peer(pub_peer_id, pub_addr.clone());
        // worker_subscriber_network.dial(pub_peer_id)?;

        // however, this works
        // worker_subscriber_network.dial(pub_addr)?;

        // spawn subscriber network
        worker_subscriber_network.spawn();

        // spawn publish network
        worker_publish_network.spawn();
        let pub_id = worker_publish_network_handle.local_peer_id().await?;
        let pub_listeners = worker_subscriber_network_handle.listeners().await?;

        worker_subscriber_network_handle
            .add_explicit_peer(
                pub_id.clone(),
                pub_listeners.first().expect("pub network is listening").clone(),
            )
            .await?;

        tokio::time::sleep(Duration::from_secs(3)).await;

        // dial peer by id now
        worker_subscriber_network_handle.dial(pub_id.into()).await?;

        // // process dial event for publisher
        // let event = worker_publish_network.network.select_next_some().await;
        // println!("publisher event :D\n{event:?}");
        // let event = worker_publish_network.network.select_next_some().await;
        // println!("publisher event :D\n{event:?}");
        // let event = worker_publish_network.network.select_next_some().await;
        // println!("publisher event :D\n{event:?}");

        // by this point, the three events needed to process peer's subscription are complete
        // let sub_addr = worker_subscriber_network_handle.listeners().await?;
        // assert!(!sub_addr.is_empty());

        // publish random block
        let random_block = WorkerBlock::default();
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);
        let res = tx_pub.send(expected_result.clone()).await;
        assert!(res.is_ok());

        let gossip_block = timeout(Duration::from_secs(5), rx_sub.recv())
            .await
            .expect("timeout waiting for gossiped worker block")
            .expect("worker block received");

        assert_eq!(gossip_block, expected_result);

        Ok(())
    }
}
