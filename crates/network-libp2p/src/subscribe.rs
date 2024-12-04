//! Gossipsub network subscriber implementation.
//!
//! Subscribers receive gossipped output from committee-voting validators.

use crate::types::{
    start_swarm, PublishMessageId, CONSENSUS_HEADER_TOPIC, PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
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
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};
use tracing::{error, trace};

/// The worker's network for publishing sealed worker blocks.
pub struct SubscriberNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for receiving sealed worker blocks to publish.
    sender: Sender<Vec<u8>>,
    // /// The [Multiaddr] for the swarm.
    // multiaddr: Multiaddr,
}

impl SubscriberNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(
        topic: IdentTopic,
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<Self>
    where
        M: PublishMessageId<'a>,
    {
        // create swarm and start listening
        let mut swarm = start_swarm::<M>(multiaddr)?;
        // subscribe to topic
        swarm.behaviour_mut().subscribe(&topic)?;
        // return Self
        Ok(Self { topic, network: swarm, sender })
    }

    /// Return this publisher's [PeerId].
    pub fn local_peer_id(&self) -> &PeerId {
        self.network.local_peer_id()
    }

    /// Return an iterator of addresses the network is listening on.
    pub fn listeners(&self) -> Vec<Multiaddr> {
        self.network.listeners().cloned().collect()
    }

    /// Add an explicit peer to support further discovery.
    pub fn add_explicit_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.network.behaviour_mut().add_explicit_peer(&peer_id);
        self.network.add_peer_address(peer_id, addr);
    }

    /// Spawn the network to process incoming gossip.
    ///
    /// Calls [`Swarm::listen_on`] and spawns `Self` as a future.
    pub fn spawn(self) -> JoinHandle<()> {
        // spawn future
        tokio::task::spawn(self)
    }

    /// Create a new subscribe network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to subscribe sealed blocks after they reach quorum.
    pub fn new_for_worker(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<Self> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new::<SealedWorkerBlock>(topic, sender, multiaddr)
    }

    /// Create a new subscribe network for [Certificate].
    ///
    /// This type is used by primary to subscribe certificates after headers reach quorum.
    pub fn new_for_primary(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<Self> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new::<Certificate>(topic, sender, multiaddr)
    }

    /// Create a new subscribe network for [ConsensusHeader].
    ///
    /// This type is used by consensus to subscribe consensus block headers after the subdag commits the latest round (finality).
    pub fn new_for_consensus(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> eyre::Result<Self> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new::<ConsensusHeader>(topic, sender, multiaddr)
    }
}

impl Future for SubscriberNetwork {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while let Some(swarm_event) = ready!(this.network.poll_next_unpin(cx)) {
            match swarm_event {
                SwarmEvent::Behaviour(gossip) => match gossip {
                    gossipsub::Event::Message { propagation_source, message_id, message } => {
                        // - `propagation_source` is the PeerId created from the  publisher's public
                        //   key
                        // - message_id is the digest of the worker block / certificate / consensus
                        //   header
                        // - message.data is the gossipped worker block / certificate / consensus
                        //   header
                        if let Err(e) = this.sender.try_send(message.data) {
                            // fatal: receiver dropped or channel queue full
                            error!(target: "subscriber-network", topic=?this.topic, ?propagation_source, ?message_id, ?e, "failed to forward received message!");
                            return Poll::Ready(());
                        }
                    }
                    gossipsub::Event::Subscribed { peer_id, topic } => {
                        trace!(target: "subscriber-network", topic=?this.topic, ?peer_id, ?topic)
                    }
                    gossipsub::Event::Unsubscribed { peer_id, topic } => {
                        trace!(target: "subscriber-network", topic=?this.topic, ?peer_id, ?topic)
                    }
                    gossipsub::Event::GossipsubNotSupported { peer_id } => {
                        // TODO: remove peer at this point?
                        trace!(target: "subscriber-network", topic=?this.topic, ?peer_id)
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
                    trace!(target: "subscriber-network", topic=?this.topic, ?peer_id, ?connection_id, ?endpoint, ?num_established, ?concurrent_dial_errors, ?established_in)
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    connection_id,
                    endpoint,
                    num_established,
                    cause,
                } => trace!(
                    target: "subscriber-network",
                    topic=?this.topic,
                    ?peer_id,
                    ?connection_id,
                    ?endpoint,
                    ?num_established,
                    ?cause,
                ),
                SwarmEvent::IncomingConnection { connection_id, local_addr, send_back_addr } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?connection_id, ?local_addr, ?send_back_addr)
                }
                SwarmEvent::IncomingConnectionError {
                    connection_id,
                    local_addr,
                    send_back_addr,
                    error,
                } => trace!(
                    target: "subscriber-network",
                    topic=?this.topic,
                    ?connection_id,
                    ?local_addr,
                    ?send_back_addr,
                    ?error,
                ),
                SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?connection_id, ?peer_id, ?error,)
                }
                SwarmEvent::NewListenAddr { listener_id, address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?listener_id, ?address)
                }
                SwarmEvent::ExpiredListenAddr { listener_id, address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?listener_id, ?address)
                }
                SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?listener_id, ?addresses, ?reason)
                }
                SwarmEvent::ListenerError { listener_id, error } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?listener_id, ?error)
                }
                SwarmEvent::Dialing { peer_id, connection_id } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ? peer_id, ?connection_id)
                }
                SwarmEvent::NewExternalAddrCandidate { address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?address)
                }
                SwarmEvent::ExternalAddrConfirmed { address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?address)
                }
                SwarmEvent::ExternalAddrExpired { address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?address)
                }
                SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?peer_id, ?address)
                }
                _e => {
                    trace!(target: "subscriber-network", topic=?this.topic, ?_e, "non-exhaustive event match")
                }
            }
        }

        Poll::Ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::SubscriberNetwork;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn todo_test() -> eyre::Result<()> {
        let (tx, _rx) = mpsc::channel(1);
        let listen_on = "/ip4/0.0.0.0/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");
        let worker_publish_network = SubscriberNetwork::new_for_worker(tx, listen_on)?;
        let _ = worker_publish_network.spawn();

        Ok(())
    }
}
