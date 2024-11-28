//! Worker gossipsub network.

use consensus_metrics::spawn_logged_monitored_task;
use eyre::eyre;
use futures::{FutureExt, StreamExt as _};
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
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

/// Generate a swarm type for use with gossip network.
pub fn build_swarm<'a, M>() -> Swarm<gossipsub::Behaviour>
where
    M: PublishMessageId<'a>,
{
    // generate a random ed25519 key
    SwarmBuilder::with_new_identity()
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
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be
                // propagated.
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
        .build()
}

/// The topic for NVVs to subscribe to for published worker blocks.
pub const WORKER_BLOCK_TOPIC: &str = "tn_worker_blocks";
/// The topic for NVVs to subscribe to for published primary certificates.
pub const PRIMARY_CERT_TOPIC: &str = "tn_certificates";
/// The topic for NVVs to subscribe to for published consensus chain.
pub const CONSENSUS_HEADER_TOPIC: &str = "tn_consensus_headers";

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

/// Convenience trait to make publish network generic over message types.
///
/// The function decodes the `[libp2p::Message]` data field and returns the digest. Using the digest
/// for published message topics makes it easier for peers to recover missing data through the
/// gossip network because the message id is the same as the data type's digest used to reach
/// consensus.
pub trait PublishMessageId<'a>: From<&'a Vec<u8>> {
    /// Create a message id for a published message to the gossip network.
    ///
    /// Lifetimes are preferred for easier maintainability.
    fn message_id(msg: &Message) -> BlockHash;
}

impl<'a> PublishMessageId<'a> for SealedWorkerBlock {
    fn message_id(msg: &Message) -> BlockHash {
        // TODO: this approach doesn't require lifetimes, but is harder to maintain.
        //
        // The tradeoff is:
        // maintainability vs readability.
        //
        // tn_types::decode::<Self>(&msg.data).digest()

        Self::from(&msg.data).digest()
    }
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

        loop {
            match this.network.poll_next_unpin(cx) {
                Poll::Ready(Some(swarm_event)) => {
                    todo!()
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

    use super::SubscriberNetwork;

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
