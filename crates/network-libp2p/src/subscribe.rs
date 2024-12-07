//! Gossipsub network subscriber implementation.
//!
//! Subscribers receive gossipped output from committee-voting validators.

use std::collections::{HashMap, HashSet};

use crate::{
    helpers::{process_network_command, start_swarm},
    types::{
        GossipNetworkHandle, GossipNetworkMessage, NetworkCommand, CONSENSUS_HEADER_TOPIC,
        PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
    },
};
use eyre::eyre;
use futures::StreamExt as _;
use libp2p::{
    gossipsub::{self, IdentTopic, MessageAcceptance, TopicScoreParams},
    swarm::SwarmEvent,
    Multiaddr, PeerId, Swarm,
};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::{error, info, trace};

/// The worker's network for publishing sealed worker blocks.
pub struct SubscriberNetwork<M> {
    /// The topic for publishing.
    topic: IdentTopic,
    /// The gossip network for flood publishing sealed worker blocks.
    network: Swarm<gossipsub::Behaviour>,
    /// The stream for forwarding downloaded messages.
    sender: Sender<M>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand>,
    /// The collection of staked validators.
    ///
    /// This set is updated at the start of each epoch and used to verify message sources are from
    /// validators.
    ///
    /// TODO: use the entire list of staked validators to prevent invalidating messages
    /// that arrive late?
    /// Or just use current CVVs.
    authorized_publishers: HashSet<PeerId>,
}

impl<M> SubscriberNetwork<M>
where
    M: GossipNetworkMessage + Send + Sync + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        topic: IdentTopic,
        sender: mpsc::Sender<M>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // create handle
        let (handle_tx, commands) = mpsc::channel(1);
        let handle = GossipNetworkHandle::new(handle_tx);

        // create swarm and start listening
        let mut swarm = start_swarm::<M>(multiaddr)?;

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
        let network = Self { topic, network: swarm, sender, commands, authorized_publishers };

        Ok((network, handle))
    }

    /// Create a new subscribe network for [SealedWorkerBlock].
    ///
    /// This type is used by worker to subscribe sealed blocks after they reach quorum.
    pub fn new_for_worker(
        sender: mpsc::Sender<M>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // worker's default topic
        let topic = gossipsub::IdentTopic::new(WORKER_BLOCK_TOPIC);
        Self::new(topic, sender, multiaddr, authorized_publishers)
    }

    /// Create a new subscribe network for [Certificate].
    ///
    /// This type is used by primary to subscribe certificates after headers reach quorum.
    pub fn new_for_primary(
        sender: mpsc::Sender<M>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // primary's default topic
        let topic = gossipsub::IdentTopic::new(PRIMARY_CERT_TOPIC);
        Self::new(topic, sender, multiaddr, authorized_publishers)
    }

    /// Create a new subscribe network for [ConsensusHeader].
    ///
    /// This type is used by consensus to subscribe consensus block headers after the subdag commits
    /// the latest round (finality).
    pub fn new_for_consensus(
        sender: mpsc::Sender<M>,
        multiaddr: Multiaddr,
        authorized_publishers: HashSet<PeerId>,
    ) -> eyre::Result<(Self, GossipNetworkHandle)> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        Self::new(topic, sender, multiaddr, authorized_publishers)
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
    async fn process_event(&mut self, event: SwarmEvent<gossipsub::Event>) -> eyre::Result<()>
    where
        M: GossipNetworkMessage,
    {
        match event {
            SwarmEvent::Behaviour(gossip) => match gossip {
                gossipsub::Event::Message { propagation_source, message_id, message } => {
                    trace!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?message, "message received from publisher");
                    // verify message was published by authorized node
                    let msg_acceptance = if message
                        .source
                        .is_some_and(|id| self.authorized_publishers.contains(&id))
                    {
                        // TODO: don't decode this here - let the receiver do this and report application score
                        if let Ok(msg) = M::decode(message.data.clone()) {
                            // forward message to handler
                            if let Err(e) = self.sender.try_send(msg) {
                                error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "failed to forward received message!");
                                // fatal - unable to process gossipped messages
                                return Err(eyre!("network receiver dropped!"));
                            }
                        }

                        MessageAcceptance::Accept
                    } else {
                        MessageAcceptance::Reject
                    };

                    println!("message received! acceptance: {msg_acceptance:?}");

                    // report message validation results
                    if let Err(e) = self.network.behaviour_mut().report_message_validation_result(
                        &message_id,
                        &propagation_source,
                        msg_acceptance,
                    ) {
                        error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "error reporting message validation result");
                    }

                    // // TODO: move this as the application score
                    // // attempt to decode the message
                    // match M::try_from(message.data) {
                    //     Ok(msg) => {
                    //         // message decoded successfully
                    //         //
                    //         // report message as valid and propagate to other peers
                    //         if let Err(e) =
                    //             self.network.behaviour_mut().report_message_validation_result(
                    //                 &message_id,
                    //                 &propagation_source,
                    //                 MessageAcceptance::Accept,
                    //             )
                    //         {
                    //             error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "error reporting message validation result");
                    //         }

                    //         // forward message to handler
                    //         if let Err(e) = self.sender.try_send(msg) {
                    //             error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "failed to forward received message!");
                    //             // fatal - unable to process gossipped messages
                    //             return Err(eyre!("network receiver dropped!"));
                    //         }
                    //     }
                    //     Err(_) => {
                    //         println!("message decoding failed!");
                    //         println!(
                    //             "prop source: {propagation_source:?}\nmessage source: {:?}",
                    //             message.source
                    //         );
                    //         // decoding failed
                    //         //
                    //         // reject the message and penalize the sender
                    //         if let Err(e) =
                    //             self.network.behaviour_mut().report_message_validation_result(
                    //                 &message_id,
                    //                 &propagation_source,
                    //                 MessageAcceptance::Reject,
                    //             )
                    //         {
                    //             error!(target: "subscriber-network", topic=?self.topic, ?propagation_source, ?message_id, ?e, "error reporting message validation result");
                    //         }
                    //     }
                    // }
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
    use super::SubscriberNetwork;
    use crate::{
        helpers::process_network_command,
        types::{GossipNetworkHandle, GossipNetworkMessage, NetworkCommand, WORKER_BLOCK_TOPIC},
        PublishNetwork,
    };
    use futures::StreamExt;
    use libp2p::{
        gossipsub::{self, IdentTopic},
        swarm::SwarmEvent,
        Multiaddr, PeerId, Swarm, SwarmBuilder,
    };
    use std::{collections::HashSet, str::FromStr, time::Duration};
    use tn_test_utils::fixture_batch_with_transactions;
    use tn_types::{BlockHash, Certificate, SealedWorkerBlock};
    use tokio::{
        sync::mpsc::{self, Receiver},
        task::JoinHandle,
        time::timeout,
    };

    /// Malicious type.
    struct MaliciousPeer {
        /// Gossipsub to publish malicious messages.
        swarm: Swarm<gossipsub::Behaviour>,
        /// The receiver for processing network handle requests.
        commands: Receiver<NetworkCommand>,
    }

    impl MaliciousPeer {
        /// Start a swarm that produces bad messages.
        fn new(multiaddr: Multiaddr) -> (Self, GossipNetworkHandle) {
            // generate a random ed25519 key
            let mut swarm = SwarmBuilder::with_new_identity()
                // tokio runtime
                .with_tokio()
                // quic protocol
                .with_quic()
                // custom behavior
                .with_behaviour(|keypair| {
                    let message_id_fn = |_: &gossipsub::Message| {
                        // generate random hashes
                        let random = BlockHash::random();
                        gossipsub::MessageId::new(random.as_ref())
                    };

                    // similar config
                    let gossipsub_config = gossipsub::ConfigBuilder::default()
                        .heartbeat_interval(Duration::from_secs(1))
                        .validation_mode(gossipsub::ValidationMode::Strict)
                        .message_id_fn(message_id_fn)
                        .build()
                        .expect("malicious gossipsub builds");

                    // build a gossipsub network behaviour
                    let network = gossipsub::Behaviour::new(
                        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                        gossipsub_config,
                    )?;

                    Ok(network)
                })
                .expect("custom network behavior")
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
                .build();

            // start listening
            swarm.listen_on(multiaddr).expect("malicious network listens on open port");

            let (handle_tx, commands) = mpsc::channel(1);
            let handle = GossipNetworkHandle::new(handle_tx);
            (Self { swarm, commands }, handle)
        }

        /// Run the network loop to process commands and advance swarm state.
        fn run(mut self) -> JoinHandle<eyre::Result<()>> {
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        event = self.swarm.select_next_some() => self.process_event(event),
                        command = self.commands.recv() => match command {
                            Some(c) => process_network_command(c, &mut self.swarm),
                            None =>  return Ok(()),
                        }
                    }
                }
            })
        }

        /// Process swarm events.
        ///
        /// Currently ignores everything.
        fn process_event(&self, event: SwarmEvent<gossipsub::Event>) {
            match event {
                SwarmEvent::Behaviour(_) => (),
                SwarmEvent::ConnectionEstablished { .. } => (),
                SwarmEvent::ConnectionClosed { .. } => (),
                SwarmEvent::IncomingConnection { .. } => (),
                SwarmEvent::IncomingConnectionError { .. } => (),
                SwarmEvent::OutgoingConnectionError { .. } => (),
                SwarmEvent::NewListenAddr { .. } => (),
                SwarmEvent::ExpiredListenAddr { .. } => (),
                SwarmEvent::ListenerClosed { .. } => (),
                SwarmEvent::ListenerError { .. } => (),
                SwarmEvent::Dialing { .. } => (),
                SwarmEvent::NewExternalAddrCandidate { .. } => (),
                SwarmEvent::ExternalAddrConfirmed { .. } => (),
                SwarmEvent::ExternalAddrExpired { .. } => (),
                SwarmEvent::NewExternalAddrOfPeer { .. } => (),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_peer_removed_after_bad_gossip() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();
        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // create publisher - validator
        let (worker_publish_network, worker_publish_network_handle) =
            PublishNetwork::new_for_worker(listen_on.clone())?;

        // create malicious peer
        let (malicious_network, malicious_peer) = MaliciousPeer::new(listen_on.clone());

        // spawn malicious peer
        malicious_network.run();
        let mal_pub_id = malicious_peer.local_peer_id().await?;

        // random peer id - represents validator well-known network key
        let cvv = PeerId::from_str("1Ad82y2W8cqi6uT37s4MorQWywyy9SUJsgHJDSbigFTYWT")?;

        // create subscriber
        let (tx_sub, mut rx_sub) = mpsc::channel::<SealedWorkerBlock>(1);
        let (worker_subscriber_network, worker_subscriber_network_handle) =
            SubscriberNetwork::new_for_worker(tx_sub, listen_on, HashSet::from([cvv]))?;

        // spawn subscriber network
        worker_subscriber_network.run();

        // yield for network to start so listeners update
        tokio::task::yield_now().await;

        let pub_listeners = malicious_peer.listeners().await?;
        let pub_addr = pub_listeners.first().expect("pub network is listening").clone();

        // dial publisher to exchange information
        worker_subscriber_network_handle.dial(pub_addr.into()).await?;

        // allow enough time for peer info to exchange from dial
        //
        // sleep seems to be the only thing that works here
        tokio::time::sleep(Duration::from_secs(1)).await;

        // // publish random bytes
        // let random_block = fixture_batch_with_transactions(10);
        // let sealed_block = random_block.seal_slow();
        // let valid_message = Vec::try_from(sealed_block.clone())?;
        // let _message_id =
        //     malicious_peer.publish(IdentTopic::new(WORKER_BLOCK_TOPIC), valid_message).await?;

        // // wait for subscriber to advance
        // let gossip_block = timeout(Duration::from_secs(5), rx_sub.recv())
        //     .await
        //     .expect("timeout waiting for gossiped worker block")
        //     .expect("worker block received");

        // assert_eq!(gossip_block, sealed_block);

        // assert peers are connected
        let peers = worker_subscriber_network_handle.connected_peers().await?;
        assert!(peers.contains(&mal_pub_id));

        println!("malicious peer id: {mal_pub_id:?}");
        // println!("malicious message id: {_message_id:?}");

        // peer score
        let malicious_peer_score =
            worker_subscriber_network_handle.peer_score(mal_pub_id.clone()).await?;
        println!("malicious peer score: {malicious_peer_score:?}");

        // publish bad bytes
        //
        // `Certificate` does not deserialize to `SealedWorkerBlock`
        let random_bytes = tn_types::encode(&Certificate::default());
        malicious_peer.publish(IdentTopic::new(WORKER_BLOCK_TOPIC), random_bytes.to_vec()).await?;

        let app_score_updated = worker_subscriber_network_handle
            .set_application_score(mal_pub_id.clone(), -10.0)
            .await?;
        assert!(app_score_updated);

        // sleep for state to advance
        tokio::time::sleep(Duration::from_secs(3)).await;

        // assert peers are disconnected
        // let peers = worker_subscriber_network_handle.connected_peers().await?;

        // TODO: explicit peers always receive broadcast
        let malicious_peer_score =
            worker_subscriber_network_handle.peer_score(mal_pub_id.clone()).await?;
        println!("malicious peer score after: {malicious_peer_score:?}");
        // assert!(!peers.contains(&mal_pub_id));

        // compare mesh vs explicit peers

        Ok(())
    }
}
