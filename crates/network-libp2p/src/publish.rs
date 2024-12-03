//! Generic abstraction for publishing (flood) to the gossipsub network.

use crate::types::{
    build_swarm, PublishMessageId, CONSENSUS_HEADER_TOPIC, PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
};
use consensus_metrics::spawn_logged_monitored_task;
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{self, IdentTopic},
    Multiaddr, Swarm,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tn_types::{Certificate, ConsensusHeader, SealedWorkerBlock};
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

    /// Create a new publish network for [ConsensusHeader].
    ///
    /// This type is used by consensus to publish consensus block headers after the subdag commits the latest round (finality).
    pub fn new_for_consensus(receiver: mpsc::Receiver<Vec<u8>>, multiaddr: Multiaddr) -> Self {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new::<ConsensusHeader>(topic, receiver, multiaddr)
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
    use crate::SubscriberNetwork;
    use libp2p::Multiaddr;
    use std::time::Duration;
    use tn_types::WorkerBlock;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    async fn test_generics_compile() -> eyre::Result<()> {
        let (tx_pub, rx_pub) = mpsc::channel(1);
        let listen_on: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        let worker_publish_network = PublishNetwork::new_for_worker(rx_pub, listen_on.clone());
        let _ = worker_publish_network.spawn();

        let (tx_sub, mut rx_sub) = mpsc::channel(1);
        let worker_subscriber_network = SubscriberNetwork::new_for_worker(tx_sub, listen_on);
        let _ = worker_subscriber_network.spawn();

        // publish random block
        let random_block = WorkerBlock::default();
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);
        let res = tx_pub.try_send(expected_result.clone());
        assert!(res.is_ok());

        let gossip_block = timeout(Duration::from_secs(5), rx_sub.recv())
            .await
            .expect("timeout waiting for gossiped worker block")
            .expect("worker block received");

        assert_eq!(gossip_block, expected_result);

        Ok(())
    }
}
