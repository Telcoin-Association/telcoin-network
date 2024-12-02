//! Gossipsub network subscriber implementation.
//!
//! Subscribers receive gossipped output from committee-voting validators.

use crate::types::{build_swarm, PublishMessageId, WORKER_BLOCK_TOPIC};
use consensus_metrics::spawn_logged_monitored_task;
use futures::{ready, StreamExt as _};
use libp2p::{
    gossipsub::{self, IdentTopic},
    swarm::SwarmEvent,
    Multiaddr, Swarm,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
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
    /// The [Multiaddr] for the swarm.
    multiaddr: Multiaddr,
}

impl SubscriberNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(sender: mpsc::Sender<Vec<u8>>, multiaddr: Multiaddr) -> Self
    where
        M: PublishMessageId<'a>,
    {
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        let swarm = build_swarm::<M>();

        Self { topic, network: swarm, sender, multiaddr }
    }

    pub async fn spawn(mut self) -> JoinHandle<()> {
        // connect to network using address
        self.network
            .listen_on(self.multiaddr.clone())
            .expect("port for worker gossip publisher available");

        // spawn future
        spawn_logged_monitored_task!(self)
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
    use tn_types::SealedWorkerBlock;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_generics_compile() -> eyre::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let listen_on = "/ip4/0.0.0.0/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");
        let worker_publish_network = SubscriberNetwork::new::<SealedWorkerBlock>(tx, listen_on);
        let _ = worker_publish_network.spawn();

        Ok(())
    }
}
