//! Worker gossipsub network.

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
use tn_types::{encode, BlockHash, SealedWorkerBlock, WorkerBlock};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

/// The worker's network for publishing sealed worker blocks.
pub struct PublishNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for receiving sealed worker blocks to publish.
    stream: ReceiverStream<Vec<u8>>,
    /// The [multiaddr] for the swarm.
    listen_on: Multiaddr,
}

/// Convenience trait to make publish network generic over message types.
///
/// The function decodes the `[libp2p::Message]` data field and returns the digest. Using the digest for published message topics makes it easier for peers to recover missing data through the gossip network because the message id is the same as the data type's digest used to reach consensus.
pub trait PublishMessageId<'a>: From<&'a Vec<u8>> {
    fn message_id(msg: &Message) -> BlockHash;
}

impl<'a> PublishMessageId<'a> for SealedWorkerBlock {
    fn message_id(msg: &Message) -> BlockHash {
        // TODO: this approach doesn't require lifetimes, but is harder to maintain. The tradeoff is maintainability vs readability.
        //
        // tn_types::decode::<Self>(&msg.data).digest()

        Self::from(&msg.data).digest()
    }
}

impl PublishNetwork {
    /// Create a new instance of Self.
    pub fn new<'a, M>(receiver: mpsc::Receiver<Vec<u8>>) -> Self
    where
        M: PublishMessageId<'a>,
    {
        // pub fn new<T: From<Vec<u8>>, M: PublishMessageId<T>>(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);

        // generate a random ed25519 key
        let swarm = SwarmBuilder::with_new_identity()
            // tokio runtime
            .with_tokio()
            // quic protocol
            .with_quic()
            // custom behavior
            .with_behaviour(|keypair| {
                // To content-address message, we can take the hash of message and use it as an ID.
                let message_id_fn = |message: &gossipsub::Message| {
                    let message_id = M::message_id(message);
                    gossipsub::MessageId::new(message_id.as_ref())
                };

                // Set a custom gossipsub configuration
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
                    .validation_mode(gossipsub::ValidationMode::Strict) // this is default - enforce message signing
                    .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                    .build()
                    .map_err(|e| {
                        error!(?e, "gossipsub publish network");
                        eyre!("failed to build gossipsub config for primary")
                    })?;

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

        let stream = ReceiverStream::new(receiver);
        let listen_on = "/ip4/0.0.0.0/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        Self { topic, network: swarm, stream, listen_on }
    }

    pub async fn spawn(mut self) -> JoinHandle<()> {
        // connect to network using address
        self.network
            .listen_on(self.listen_on.clone())
            .expect("port for worker gossip publisher available");

        // spawn future
        spawn_logged_monitored_task!(self)
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
    use tn_types::SealedWorkerBlock;
    use tokio::sync::mpsc;

    use super::PublishNetwork;

    #[tokio::test]
    async fn test_generics_compile() -> eyre::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let worker_publish_network = PublishNetwork::new::<SealedWorkerBlock>(rx);
        let _ = worker_publish_network.spawn();

        Ok(())
    }
}
