//! Gossipsub network subscriber implementation.
//!
//! Subscribers receive gossipped output from committee-voting validators.

use crate::{
    helpers::{process_swarm_command, start_swarm, subscriber_gossip_config},
    types::{
        GossipNetworkHandle, NetworkCommand, CONSENSUS_HEADER_TOPIC, PRIMARY_CERT_TOPIC,
        WORKER_BLOCK_TOPIC,
    },
};
use eyre::eyre;
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{self, IdentTopic, MessageAcceptance, TopicScoreParams},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use std::collections::{HashMap, HashSet};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::{error, info, trace};

/// The worker's network for publishing sealed worker blocks.
pub struct SubscriberNetwork {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for forwarding downloaded messages.
    sender: Sender<Vec<u8>>,
    /// The sender for network handles.
    handle: Sender<NetworkCommand>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand>,
    /// The collection of staked validators.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify message sources
    /// are from validators.
    authorized_publishers: HashSet<PeerId>,
}

impl SubscriberNetwork {
    /// Create a new instance of Self.
    pub fn new(
        topic: IdentTopic,
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
        gossipsub_config: gossipsub::Config,
    ) -> eyre::Result<Self> {
        // create handle
        let (handle, commands) = mpsc::channel(1);

        // create swarm and start listening
        let mut swarm = start_swarm(multiaddr, gossipsub_config)?;

        // configure peer score parameters
        //
        // default for now
        let score_params = gossipsub::PeerScoreParams {
            topics: HashMap::from([(topic.hash(), TopicScoreParams::default())]),
            ..Default::default()
        };

        // configure thresholds at which peers are considered faulty or malicious
        //
        // peer baseline is 0
        let score_thresholds = gossipsub::PeerScoreThresholds {
            gossip_threshold: -10.0,   // ignore gossip to and from peer
            publish_threshold: -20.0,  // don't flood publish to this peer
            graylist_threshold: -50.0, // effectively ignore peer
            accept_px_threshold: 10.0, // score only attainable by validators
            opportunistic_graft_threshold: 5.0,
        };

        // enable peer scoring
        swarm.behaviour_mut().with_peer_score(score_params, score_thresholds).map_err(|e| {
            error!(?e, "gossipsub publish network");
            eyre!("failed to set peer score for gossipsub")
        })?;

        // subscribe to topic
        swarm.behaviour_mut().subscribe(&topic)?;

        // create Self
        let network =
            Self { topic, network: swarm, sender, handle, commands, authorized_publishers };

        Ok(network)
    }

    /// Return a [GossipNetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> GossipNetworkHandle {
        GossipNetworkHandle::new(self.handle.clone())
    }

    /// Create a new subscribe network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to subscribe sealed blocks after they reach quorum.
    pub fn default_for_worker(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<Self> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        // default gossipsub config
        let gossipsub_config = subscriber_gossip_config()?;
        Self::new(topic, sender, multiaddr, authorized_publishers, gossipsub_config)
    }

    /// Create a new subscribe network for [Certificate].
    ///
    /// This type is used by primary to subscribe certificates after headers reach quorum.
    pub fn default_for_primary(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<Self> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        // default gossipsub config
        let gossipsub_config = subscriber_gossip_config()?;
        Self::new(topic, sender, multiaddr, authorized_publishers, gossipsub_config)
    }

    /// Create a new subscribe network for [ConsensusHeader].
    ///
    /// This type is used by consensus to subscribe consensus block headers after the subdag commits
    /// the latest round (finality).
    pub fn default_for_consensus(
        sender: mpsc::Sender<Vec<u8>>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<Self> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        // default gossipsub config
        let gossipsub_config = subscriber_gossip_config()?;
        Self::new(topic, sender, multiaddr, authorized_publishers, gossipsub_config)
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
        match command {
            NetworkCommand::UpdateAuthorizedPublishers { authorities, reply } => {
                self.authorized_publishers = authorities;
                let _ = reply.send(Ok(()));
            }
            NetworkCommand::Swarm(c) => process_swarm_command(c, &mut self.network),
        }
    }

    /// Process events from the swarm.
    async fn process_event(&mut self, event: SwarmEvent<gossipsub::Event>) -> eyre::Result<()> {
        match event {
            SwarmEvent::Behaviour(gossip) => match gossip {
                gossipsub::Event::Message { propagation_source, message_id, message } => {
                    trace!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?message, "message received from publisher");
                    // verify message was published by authorized node
                    let msg_acceptance = if message
                        .source
                        .is_some_and(|id| self.authorized_publishers.contains(&id))
                    {
                        // forward message to handler
                        if let Err(e) = self.sender.try_send(message.data) {
                            error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "failed to forward received message!");
                            // fatal - unable to process gossipped messages
                            return Err(eyre!("network receiver dropped!"));
                        }

                        MessageAcceptance::Accept
                    } else {
                        MessageAcceptance::Reject
                    };

                    // report message validation results
                    if let Err(e) = self.network.behaviour_mut().report_message_validation_result(
                        &message_id,
                        &propagation_source,
                        msg_acceptance,
                    ) {
                        error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "error reporting message validation result");
                    }
                }
                gossipsub::Event::Subscribed { peer_id, topic } => {
                    trace!(target: "subscriber-network", topic=?self.topic, ?peer_id, ?topic, "gossipsub event - subscribed")
                }
                gossipsub::Event::Unsubscribed { peer_id, topic } => {
                    trace!(target: "subscriber-network", topic=?self.topic, ?peer_id, ?topic, "gossipsub event - unsubscribed")
                }
                gossipsub::Event::GossipsubNotSupported { peer_id } => {
                    // TODO: remove peer at self point?
                    trace!(target: "subscriber-network", topic=?self.topic, ?peer_id, "gossipsub event - not supported")
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
                trace!(target: "subscriber-network", topic=?self.topic, ?peer_id, ?connection_id, ?endpoint, ?num_established, ?concurrent_dial_errors, ?established_in, "connection established")
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                cause,
            } => trace!(
                target: "subscriber-network",
                topic=?self.topic,
                ?peer_id,
                ?connection_id,
                ?endpoint,
                ?num_established,
                ?cause,
                "connection closed"
            ),
            SwarmEvent::IncomingConnection { connection_id, local_addr, send_back_addr } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?connection_id, ?local_addr, ?send_back_addr, "incoming connection")
            }
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                error,
            } => trace!(
                target: "subscriber-network",
                topic=?self.topic,
                ?connection_id,
                ?local_addr,
                ?send_back_addr,
                ?error,
                "incoming connection error"
            ),
            SwarmEvent::OutgoingConnectionError { connection_id, peer_id, error } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?connection_id, ?peer_id, ?error, "outgoing connection error")
            }
            SwarmEvent::NewListenAddr { listener_id, address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?listener_id, ?address, "new listener addr")
            }
            SwarmEvent::ExpiredListenAddr { listener_id, address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?listener_id, ?address, "expired listen addr")
            }
            SwarmEvent::ListenerClosed { listener_id, addresses, reason } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?listener_id, ?addresses, ?reason, "listener closed")
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?listener_id, ?error, "listener error")
            }
            SwarmEvent::Dialing { peer_id, connection_id } => {
                trace!(target: "subscriber-network", topic=?self.topic, ? peer_id, ?connection_id, "dialing")
            }
            SwarmEvent::NewExternalAddrCandidate { address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?address, "new external addr candidate")
            }
            SwarmEvent::ExternalAddrConfirmed { address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?address, "external addr confirmed")
            }
            SwarmEvent::ExternalAddrExpired { address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?address, "external addr expired")
            }
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                trace!(target: "subscriber-network", topic=?self.topic, ?peer_id, ?address, "new external addr of peer")
            }
            _e => {
                trace!(target: "subscriber-network", topic=?self.topic, ?_e, "non-exhaustive event match")
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{types::PRIMARY_CERT_TOPIC, PublishNetwork, SubscriberNetwork};
    use libp2p::{gossipsub::IdentTopic, Multiaddr};
    use std::{collections::HashSet, time::Duration};
    use tn_test_utils::fixture_batch_with_transactions;
    use tokio::{sync::mpsc, time::timeout};

    #[tokio::test]
    async fn test_msg_verification_ignores_unauthorized_publisher() -> eyre::Result<()> {
        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // create publisher
        let cvv_network = PublishNetwork::default_for_primary(listen_on.clone())?;
        let cvv = cvv_network.network_handle();

        // spawn publish network
        cvv_network.run();

        // obtain publisher's peer id
        let cvv_id = cvv.local_peer_id().await?;

        // create subscriber
        let (tx_sub, mut rx_sub) = mpsc::channel(1);
        let nvv_network =
            SubscriberNetwork::default_for_primary(tx_sub, listen_on, HashSet::from([cvv_id]))?;
        let nvv = nvv_network.network_handle();

        // spawn subscriber network
        nvv_network.run();

        // yield for network to start so listeners update
        tokio::task::yield_now().await;

        let pub_listeners = cvv.listeners().await?;
        let pub_addr = pub_listeners.first().expect("pub network is listening").clone();

        // dial publisher to exchange information
        nvv.dial(pub_addr.into()).await?;

        // allow enough time for peer info to exchange from dial
        //
        // sleep seems to be the only thing that works here
        tokio::time::sleep(Duration::from_secs(2)).await;

        // publish random block
        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);
        let _message_id =
            cvv.publish(IdentTopic::new(PRIMARY_CERT_TOPIC), expected_result.clone()).await?;

        // wait for subscriber to forward
        let gossip_block = timeout(Duration::from_secs(2), rx_sub.recv())
            .await
            .expect("timeout waiting for gossiped worker block")
            .expect("worker block received");

        assert_eq!(gossip_block, expected_result);

        // remove cvv from whitelist and try to publish again
        nvv.update_authorized_publishers(HashSet::with_capacity(0)).await?;

        let random_block = fixture_batch_with_transactions(10);
        let sealed_block = random_block.seal_slow();
        let expected_result = Vec::from(&sealed_block);
        let _message_id =
            cvv.publish(IdentTopic::new(PRIMARY_CERT_TOPIC), expected_result.clone()).await?;

        // message should never be forwarded
        let timeout = timeout(Duration::from_secs(2), rx_sub.recv()).await;
        assert!(timeout.is_err());

        // TODO: assert peer score after bad message

        Ok(())
    }
}
