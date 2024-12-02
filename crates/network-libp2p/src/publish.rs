//! Generic abstraction for publishing (flood) to the gossipsub network.

use crate::types::{build_swarm, PublishMessageId, PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC};
use consensus_metrics::spawn_logged_monitored_task;
use eyre::eyre;
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{self, IdentTopic, Message},
    Multiaddr, Swarm, SwarmBuilder,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tn_types::{BlockHash, Certificate, SealedWorkerBlock};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

/// The worker's network for publishing sealed worker blocks.
pub struct PublishNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for receiving sealed worker blocks to publish.
    stream: ReceiverStream<Vec<u8>>,
    /// The [Multiaddr] for the swarm.
    multiaddr: Multiaddr,
}

impl PublishNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(
        topic: IdentTopic,
        receiver: mpsc::Receiver<Vec<u8>>,
        multiaddr: Multiaddr,
    ) -> Self
    where
        M: PublishMessageId<'a>,
    {
        let swarm = build_swarm::<M>();

        // convert receiver into stream for convenience in `Self::poll`
        let stream = ReceiverStream::new(receiver);

        Self { topic, network: swarm, stream, multiaddr }
    }

    /// Spawn the network and start listening.
    ///
    /// Calls [`Swarm::listen_on`] and spawns `Self` as a future.
    pub async fn spawn(mut self) -> JoinHandle<()> {
        // connect to network using address
        self.network
            .listen_on(self.multiaddr.clone())
            .expect("port for gossip publisher available");

        // spawn future
        spawn_logged_monitored_task!(self)
    }

    /// Create a new publish network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to publish sealed blocks after they reach quorum.
    pub fn new_for_worker(receiver: mpsc::Receiver<Vec<u8>>, multiaddr: Multiaddr) -> Self {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new::<SealedWorkerBlock>(topic, receiver, multiaddr)
    }

    /// Create a new publish network for [Certificate].
    ///
    /// This type is used by primary to publish certificates after headers reach quorum.
    pub fn new_for_primary(receiver: mpsc::Receiver<Vec<u8>>, multiaddr: Multiaddr) -> Self {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new::<Certificate>(topic, receiver, multiaddr)
    }
}

impl Future for PublishNetwork {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(worker_block)) => {
                    if let Err(e) =
                        this.network.behaviour_mut().publish(this.topic.clone(), worker_block)
                    {
                        error!(target: "worker::gossip::publish", ?e);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::PublishNetwork;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_generics_compile() -> eyre::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let listen_on = "/ip4/0.0.0.0/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");
        // let worker_publish_network = PublishNetwork::new::<SealedWorkerBlock>(rx, listen_on);

        let worker_publish_network = PublishNetwork::new_for_worker(rx, listen_on);
        let _ = worker_publish_network.spawn();

        Ok(())
    }
}
