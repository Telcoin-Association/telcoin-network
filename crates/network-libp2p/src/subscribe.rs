//! Gossipsub network subscriber implementation.
//!
//! Subscribers receive gossipped output from committee-voting validators.

use crate::{
    error::NetworkError,
    helpers::{process_swarm_command, start_swarm, subscriber_gossip_config},
    types::{
        GossipNetworkHandle, NetworkCommand, NetworkResult, CONSENSUS_HEADER_TOPIC,
        PRIMARY_CERT_TOPIC, WORKER_BLOCK_TOPIC,
    },
};
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
    ) -> NetworkResult<Self> {
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
        //
        // TODO: make these constants
        let score_thresholds = gossipsub::PeerScoreThresholds {
            gossip_threshold: -10.0,    // ignore gossip to and from peer
            publish_threshold: -20.0,   // don't flood publish to this peer
            graylist_threshold: -50.0,  // effectively ignore peer
            accept_px_threshold: 100.0, // score only attainable by validators
            opportunistic_graft_threshold: 5.0,
        };

        // enable peer scoring
        swarm.behaviour_mut().with_peer_score(score_params, score_thresholds).map_err(|e| {
            error!(?e, "gossipsub publish network");
            NetworkError::EnablePeerScoreBehavior(e)
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
    ) -> NetworkResult<Self> {
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
    ) -> NetworkResult<Self> {
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
    ) -> NetworkResult<Self> {
        // consensus header's default topic
        let topic = gossipsub::IdentTopic::new(CONSENSUS_HEADER_TOPIC);
        // default gossipsub config
        let gossipsub_config = subscriber_gossip_config()?;
        Self::new(topic, sender, multiaddr, authorized_publishers, gossipsub_config)
    }

    /// Run the network loop to process incoming gossip.
    pub fn run(mut self) -> JoinHandle<NetworkResult<()>> {
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
    async fn process_event(&mut self, event: SwarmEvent<gossipsub::Event>) -> NetworkResult<()> {
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
                            return Err(e.into());
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
    use crate::{
        helpers::{process_swarm_command, subscriber_gossip_config},
        types::{
            GossipNetworkHandle, NetworkCommand, NetworkResult, PRIMARY_CERT_TOPIC,
            WORKER_BLOCK_TOPIC,
        },
        PublishNetwork, SubscriberNetwork,
    };
    use futures::StreamExt as _;
    use libp2p::{
        gossipsub::{self, IdentTopic},
        swarm::SwarmEvent,
        Multiaddr, Swarm, SwarmBuilder,
    };
    use std::{collections::HashSet, time::Duration};
    use tn_test_utils::fixture_batch_with_transactions;
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
        fn new(
            multiaddr: Multiaddr,
            gossipsub_config: gossipsub::Config,
        ) -> (Self, GossipNetworkHandle) {
            // generate a random ed25519 key
            let mut swarm = SwarmBuilder::with_new_identity()
                // tokio runtime
                .with_tokio()
                // quic protocol
                .with_quic()
                // custom behavior
                .with_behaviour(|keypair| {
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
        fn run(mut self) -> JoinHandle<NetworkResult<()>> {
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        event = self.swarm.select_next_some() => self.process_event(event),
                        command = self.commands.recv() => match command {
                            Some(command) => match command {
                                NetworkCommand::Swarm(c) =>  process_swarm_command(c, &mut self.swarm),
                                _ => (),
                            }
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
    async fn test_msg_verification_ignores_unauthorized_publisher() -> NetworkResult<()> {
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

        let cvv_listeners = cvv.listeners().await?;
        let cvv_addr = cvv_listeners.first().expect("pub network is listening").clone();

        // dial publisher to exchange information
        nvv.dial(cvv_addr.into()).await?;

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

        Ok(())
    }

    // here's how this should work:
    // - cvv starts network
    // - honest node joins to receive gossip
    // - malicious (mal) node dials cvv to join network
    // - cvv does px with mal node to simulate peer discovery in production
    // - mal node becomes mesh peer with honest node
    // - mal node tries to publish message
    // - mal node gets kicked from honest node mesh
    #[tokio::test]
    async fn test_peer_exchange() -> NetworkResult<()> {
        // TODO: remove this
        reth_tracing::init_test_tracing();

        // default any address
        let listen_on: Multiaddr = "/ip4/127.0.0.1/udp/0/quic-v1"
            .parse()
            .expect("multiaddr parsed for worker gossip publisher");

        // create pub config
        let cvv_config = gossipsub::ConfigBuilder::default()
            // same as crate::helpers::publisher_gossip_config()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .do_px()
            // lower number of peers from default to simulate max peer connections
            .mesh_n(1) // target # peers
            .mesh_outbound_min(0)
            .mesh_n_low(0) // min number of peers
            .mesh_n_high(1) // max number of peers
            .prune_peers(1)
            .build()?;

        // create publisher
        let test_topic = IdentTopic::new(WORKER_BLOCK_TOPIC);
        let cvv_network = PublishNetwork::new(test_topic.clone(), listen_on.clone(), cvv_config)?;
        let cvv = cvv_network.network_handle();

        // spawn publish network
        cvv_network.run();

        // obtain publisher's peer id
        let cvv_id = cvv.local_peer_id().await?;

        // create subscriber
        let (tx_sub, _rx_sub) = mpsc::channel(1);
        let honest_network = SubscriberNetwork::default_for_worker(
            tx_sub,
            listen_on.clone(),
            HashSet::from([cvv_id]),
        )?;

        let honest_peer = honest_network.network_handle();

        // spawn subscriber network
        honest_network.run();

        // yield for network to start so listeners update
        tokio::task::yield_now().await;

        let cvv_listeners = cvv.listeners().await?;
        let cvv_addr = cvv_listeners.first().expect("pub network is listening").clone();

        // dial publisher to exchange information
        honest_peer.dial(cvv_addr.clone().into()).await?;

        // sleep for 3 heartbeat
        tokio::time::sleep(Duration::from_secs(3)).await;

        // set bootnode score very high
        let res = honest_peer.set_application_score(cvv_id.clone(), 100.0).await?;
        assert!(res);

        // assert peer score
        let cvvs_peer_score = honest_peer.peer_score(cvv_id.clone()).await?;
        println!("cvvs_peer_score (honest): {cvvs_peer_score:?}");
        assert_eq!(cvvs_peer_score, Some(1000.0));

        // allow enough time for peer info to exchange from dial
        //
        // 1 heartbeat interval
        tokio::time::sleep(Duration::from_secs(3)).await;

        let connected_cvv_peers = cvv.connected_peers().await?;
        println!("\n\nconnected cvv peers: {connected_cvv_peers:?}\n\n");
        let all_cvv_peers = cvv.all_peers().await?;
        println!("\n\nall cvv peers: {all_cvv_peers:?}\n\n");

        // // publish random block
        // let random_block = fixture_batch_with_transactions(10);
        // let sealed_block = random_block.seal_slow();
        // let expected_result = Vec::from(&sealed_block);
        // let _message_id =
        //     cvv.publish(IdentTopic::new(PRIMARY_CERT_TOPIC), expected_result.clone()).await?;

        // // wait for subscriber to forward
        // let gossip_block = timeout(Duration::from_secs(2), rx_sub.recv())
        //     .await
        //     .expect("timeout waiting for gossiped worker block")
        //     .expect("worker block received");

        // assert_eq!(gossip_block, expected_result);

        // create malicious peer
        let default_sub_config = subscriber_gossip_config()?;
        let (malicious_network, malicious_peer) = MaliciousPeer::new(listen_on, default_sub_config);

        // set cvv (bootstrap) peer score high
        //
        // TODO: make these scores consts
        malicious_network.run();

        // subscribe to gossip
        let res = malicious_peer.subscribe(test_topic).await?;
        assert!(res);

        malicious_peer.dial(cvv_addr.clone().into()).await?;

        // sleep for 3 heartbeat intervals
        tokio::time::sleep(Duration::from_secs(3)).await;

        // set bootnode score very high
        let res = malicious_peer.set_application_score(cvv_id.clone(), 100.0).await?;
        assert!(res);

        // assert peer score
        let cvvs_peer_score = malicious_peer.peer_score(cvv_id.clone()).await?;
        println!("cvvs_peer_score (mal): {cvvs_peer_score:?}");
        assert_eq!(cvvs_peer_score, Some(1000.0));

        let mal_id = malicious_peer.local_peer_id().await?;
        let honest_id = malicious_peer.local_peer_id().await?;

        println!("cvv_id: {cvv_id:?}");
        println!("honest_id: {honest_id:?}");
        println!("malicious_id: {mal_id:?}\n");

        // all peers for debug

        // assert cvv's peers
        let peers = cvv.connected_peers().await?;
        println!("cvv node's peers: {peers:?}\n");
        assert!(peers.contains(&honest_id));
        assert!(peers.contains(&mal_id));

        // assert honest node's peers
        let peers = honest_peer.connected_peers().await?;
        println!("honest node's peers: {peers:?}\n");
        assert!(peers.contains(&cvv_id));
        // assert!(peers.contains(&mal_id));

        // assert malicious node's peers
        let peers = malicious_peer.connected_peers().await?;
        println!("mal node's peers: {peers:?}\n");
        assert!(peers.contains(&cvv_id));
        // assert!(peers.contains(&honest_id));

        // peer score
        let malicious_peer_score = honest_peer.peer_score(mal_id.clone()).await?;
        println!("malicious peer score: {malicious_peer_score:?}");

        // publish bad bytes
        //
        // `Certificate` does not deserialize to `SealedWorkerBlock`
        let _random_bytes = tn_types::encode(&tn_types::Certificate::default());
        // malicious_peer.publish(IdentTopic::new(WORKER_BLOCK_TOPIC), random_bytes.to_vec()).await?;

        // sleep for state to advance
        tokio::time::sleep(Duration::from_secs(3)).await;

        // assert peers are disconnected
        // let peers = honest_peer.connected_peers().await?;

        // TODO: explicit peers always receive broadcast
        let malicious_peer_score = honest_peer.peer_score(mal_id.clone()).await?;
        println!("malicious peer score after: {malicious_peer_score:?}");
        // assert!(!peers.contains(&mal_id));

        // compare mesh vs explicit peers
        let mal_peers = malicious_peer.connected_peers().await?;
        let honest_peers = honest_peer.connected_peers().await?;
        let cvv_peers = cvv.connected_peers().await?;
        println!("cvv_peers connected: {cvv_peers:?}");
        println!("honest_peers connected: {honest_peers:?}");
        println!("mal_peers connected: {mal_peers:?}\n");

        // honest peer
        let honest_all_peers = honest_peer.all_peers().await?;
        println!("honest_all_peers: {honest_all_peers:?}");
        let honest_all_mesh_peers = honest_peer.all_mesh_peers().await?;
        println!("honest_all_mesh_peers: {honest_all_mesh_peers:?}\n");

        // malicious peer
        let mal_all_peers = malicious_peer.all_peers().await?;
        println!("mal_all_peers: {mal_all_peers:?}");
        let mal_all_mesh_peers = malicious_peer.all_mesh_peers().await?;
        println!("mal_all_mesh_peers: {mal_all_mesh_peers:?}\n");

        // cvv
        let cvv_all_peers = cvv.all_peers().await?;
        println!("cvv_all_peers: {cvv_all_peers:?}");
        let cvv_all_mesh_peers = cvv.all_mesh_peers().await?;
        println!("cvv_all_mesh_peers: {cvv_all_mesh_peers:?}");

        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(())
    }
}
