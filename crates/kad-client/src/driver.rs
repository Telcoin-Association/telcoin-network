//! The background task that owns the libp2p swarm behind a [`KadClient`](crate::KadClient).
//!
//! The driver is the only place the swarm is polled. Callers talk to it through a bounded command
//! channel and receive results on per-request oneshots, so the swarm is never shared across tasks
//! and never blocked on a caller.

use crate::{
    error::KadClientError,
    verify::{fold_newest, verify_record, VerifiedRecord},
    KadClientConfig,
};
use futures::StreamExt as _;
use libp2p::{
    gossipsub, identity, kad,
    multiaddr::Protocol,
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};
use tn_node_record::{gossip_protocol_id_prefix, BlsPublicKey, RecordDomain};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::{debug, trace};

/// Maximum encoded kademlia message size in bytes, including the record and protocol overhead.
///
/// Mirrors the node's `MAX_KAD_PACKET_SIZE` (`crates/network-libp2p/src/consensus.rs`): the node
/// pins its codec to 16 KiB, so a record the node can serve always fits, and a larger inbound
/// message can only be garbage.
const MAX_KAD_PACKET_SIZE: usize = 16 * 1024;

/// Capacity of the command channel between [`KadClient`](crate::KadClient) handles and the driver.
///
/// Commands are cheap and the driver services them promptly; the bound only applies back-pressure
/// if a caller fans out far wider than the concurrency
/// [`KadClient::get_node_records`](crate::KadClient::get_node_records) allows.
const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// The outcome of one lookup, as delivered to the caller.
pub(crate) type LookupResult = Result<Option<VerifiedRecord>, KadClientError>;

/// The client swarm's behaviours.
///
/// Kademlia does the work. Gossipsub is carried only so the node's connection admission does not
/// reject the client: the node's gossipsub dials every new connection on its chain-namespaced
/// `/tn-meshsub-{chain_id}/1.x.0` ids and, when negotiation fails, the node applies a fatal
/// penalty — banning the peer *and its IP* (`GossipEvent::GossipsubNotSupported` in
/// `crates/network-libp2p/src/consensus.rs`). A client without gossipsub therefore gets exactly
/// one lookup in before it is banned for the node's ban duration. The client never subscribes to
/// a topic and never publishes, so no gossip flows; every message it might still be handed is
/// dropped without forwarding.
#[derive(NetworkBehaviour)]
struct ClientBehaviour {
    /// The DHT reader.
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    /// Connection-admission compatibility only; see the struct docs.
    gossipsub: gossipsub::Behaviour,
}

/// A request from a [`KadClient`](crate::KadClient) handle to the driver.
#[derive(Debug)]
pub(crate) enum Command {
    /// Look up the record published under `key` and reply with the newest valid copy.
    ///
    /// The key is boxed so the enum stays small (clippy `large_enum_variant`): a
    /// [`BlsPublicKey`] carries the decompressed point alongside its 96 bytes.
    GetRecord {
        /// The BLS key the record is published under.
        key: Box<BlsPublicKey>,
        /// Where to deliver the outcome.
        reply: oneshot::Sender<LookupResult>,
    },
    /// Report which bootstrap peers currently have an established connection.
    ConnectedBootstrapPeers {
        /// Where to deliver the list.
        reply: oneshot::Sender<Vec<PeerId>>,
    },
    /// Stop polling the swarm and exit the task.
    Shutdown,
}

/// The state of one in-flight `GET_VALUE` lookup.
struct PendingQuery {
    /// The key the caller asked for; every returned copy is checked against it.
    key: BlsPublicKey,
    /// The newest valid copy seen so far.
    best: Option<VerifiedRecord>,
    /// Every copy the DHT returned, valid or not. Distinguishes "no record exists" from "records
    /// exist but none verify".
    copies_returned: usize,
    /// When the client gives up on this lookup regardless of what the DHT layer reports.
    deadline: Instant,
    /// Where to deliver the outcome.
    reply: oneshot::Sender<LookupResult>,
}

/// How a lookup ended at the DHT layer.
enum Terminal {
    /// The iterative lookup ran to completion; `stats` say how many peers answered.
    Finished(kad::QueryStats),
    /// The lookup ran past its deadline.
    Timeout,
}

/// Owns the swarm and services commands until shut down.
pub(crate) struct Driver {
    /// The swarm: a client-mode kademlia instance plus a topic-less gossipsub instance.
    swarm: Swarm<ClientBehaviour>,
    /// Requests from client handles.
    commands: mpsc::Receiver<Command>,
    /// In-flight lookups keyed by the kademlia query that serves them.
    pending: HashMap<kad::QueryId, PendingQuery>,
    /// The `(chain, role)` domain every returned record must be signed for.
    domain: RecordDomain,
    /// The kademlia protocol name this client negotiates on, for diagnostics.
    protocol: String,
    /// Per-lookup deadline.
    query_timeout: Duration,
    /// Bootstrap peers and how many established connections each currently has.
    bootstrap: BTreeMap<PeerId, u32>,
    /// Bootstrap peers whose most recent dial attempt failed outright.
    dial_failed: BTreeSet<PeerId>,
}

impl Driver {
    /// Build the swarm from `config`, seed the routing table, and start dialing every bootstrap
    /// peer. Nothing is polled until [`Self::run`].
    pub(crate) fn build(
        config: &KadClientConfig,
    ) -> Result<(Self, mpsc::Sender<Command>), KadClientError> {
        if config.bootstrap.is_empty() {
            return Err(KadClientError::NoBootstrapPeers);
        }
        let mut addresses: BTreeMap<PeerId, Vec<Multiaddr>> = BTreeMap::new();
        for addr in &config.bootstrap {
            let peer = parse_bootstrap_addr(addr)?;
            addresses.entry(peer).or_default().push(addr.clone());
        }

        // the identity is ephemeral: the client never listens, never publishes, and is never
        // inserted into a server's routing table, so a stable id would buy nothing
        let keypair = identity::Keypair::generate_ed25519();
        let local_peer_id = keypair.public().to_peer_id();

        let protocol_name = config.network_type.kad_protocol_name(config.chain_id);
        let protocol = StreamProtocol::try_from_owned(protocol_name.clone())
            .map_err(|e| KadClientError::Transport(format!("invalid kad protocol name: {e}")))?;

        let mut kad_config = kad::Config::new(protocol);
        kad_config
            // match the node's codec limit so an oversized inbound message is dropped at the same
            // boundary the node drops it
            .set_max_packet_size(MAX_KAD_PACKET_SIZE)
            .set_query_timeout(config.query_timeout)
            // a reader must never write back into the DHT
            .set_caching(kad::Caching::Disabled)
            // defense in depth: a record pushed to us is surfaced as an event and never stored
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            // no periodic self-lookups: the routing table only needs the bootstrap peers. libp2p
            // still runs one automatic bootstrap shortly after a routing-table insert (the throttle
            // is not configurable outside the crate's tests); it is a read-only FIND_NODE walk
            .set_periodic_bootstrap_interval(None);

        let mut kademlia = kad::Behaviour::with_config(
            local_peer_id,
            kad::store::MemoryStore::new(local_peer_id),
            kad_config,
        );
        // client mode: never advertise the protocol on inbound streams, so servers never insert
        // this peer into their routing tables and never route lookups through it
        kademlia.set_mode(Some(kad::Mode::Client));
        for (peer, addrs) in &addresses {
            for addr in addrs {
                // the `/p2p/` suffix is kept: libp2p accepts it and uses it to authenticate dials
                kademlia.add_address(peer, addr.clone());
            }
        }

        let gossipsub = build_gossipsub(&keypair, config.chain_id)?;

        // the node accepts QUIC only, so the client offers nothing else
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_behaviour(|_| ClientBehaviour { kademlia, gossipsub })
            .map_err(|e| KadClientError::Transport(format!("failed to build swarm: {e}")))?
            .build();

        let mut dial_failed = BTreeSet::new();
        for (peer, addrs) in &addresses {
            let opts = DialOpts::peer_id(*peer).addresses(addrs.clone()).build();
            if let Err(error) = swarm.dial(opts) {
                debug!(target: "kad-client", %peer, ?error, "bootstrap dial rejected");
                dial_failed.insert(*peer);
            }
        }
        if dial_failed.len() == addresses.len() {
            return Err(KadClientError::NoBootstrapPeerReachable);
        }

        let (tx, commands) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        let driver = Self {
            swarm,
            commands,
            pending: HashMap::new(),
            domain: config.record_domain(),
            protocol: protocol_name,
            query_timeout: config.query_timeout,
            bootstrap: addresses.keys().map(|peer| (*peer, 0)).collect(),
            dial_failed,
        };
        Ok((driver, tx))
    }

    /// Drive the swarm until shut down.
    ///
    /// First waits for a bootstrap connection and reports the outcome on `ready`; on failure the
    /// task exits without servicing commands. Every in-flight lookup is dropped on exit, which
    /// resolves its caller with [`KadClientError::Shutdown`].
    pub(crate) async fn run(mut self, ready: oneshot::Sender<Result<(), KadClientError>>) {
        let outcome = self.await_bootstrap().await;
        let failed = outcome.is_err();
        // the spawner may have given up waiting; nothing to do about it either way
        let _ = ready.send(outcome);
        if failed {
            return;
        }

        loop {
            let (has_deadline, next_deadline) = match self.next_deadline() {
                Some(deadline) => (true, deadline),
                None => (false, Instant::now()),
            };
            tokio::select! {
                event = self.swarm.select_next_some() => self.handle_swarm_event(event),
                command = self.commands.recv() => match command {
                    Some(Command::GetRecord { key, reply }) => self.start_lookup(*key, reply),
                    Some(Command::ConnectedBootstrapPeers { reply }) => {
                        let _ = reply.send(self.connected_bootstrap_peers());
                    }
                    Some(Command::Shutdown) | None => break,
                },
                _ = tokio::time::sleep_until(next_deadline), if has_deadline => self.expire_lookups(),
            }
        }
        debug!(target: "kad-client", "driver shutting down");
    }

    /// Poll the swarm until at least one bootstrap peer connects.
    ///
    /// Fails as soon as every bootstrap peer's dial has failed, or when the query timeout elapses
    /// with none connected, so a wrong address, port, or role surfaces at spawn rather than as an
    /// opaque lookup timeout later.
    async fn await_bootstrap(&mut self) -> Result<(), KadClientError> {
        let deadline = Instant::now() + self.query_timeout;
        loop {
            if self.bootstrap.values().any(|connections| *connections > 0) {
                return Ok(());
            }
            if self.dial_failed.len() == self.bootstrap.len() {
                return Err(KadClientError::NoBootstrapPeerReachable);
            }
            tokio::select! {
                event = self.swarm.select_next_some() => self.handle_swarm_event(event),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(KadClientError::NoBootstrapPeerReachable);
                }
            }
        }
    }

    /// The earliest client-side deadline among in-flight lookups.
    fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.deadline).min()
    }

    /// Bootstrap peers with at least one established connection.
    fn connected_bootstrap_peers(&self) -> Vec<PeerId> {
        self.bootstrap
            .iter()
            .filter(|(_, connections)| **connections > 0)
            .map(|(peer, _)| *peer)
            .collect()
    }

    /// Issue a `GET_VALUE` lookup for `key` and track it until it resolves.
    fn start_lookup(&mut self, key: BlsPublicKey, reply: oneshot::Sender<LookupResult>) {
        // the record key is the raw 96-byte compressed BLS public key, exactly as the node
        // publishes it (`kad::RecordKey::new(&bls_public_key)` in consensus.rs)
        let query_id = self.swarm.behaviour_mut().kademlia.get_record(kad::RecordKey::new(&key));
        trace!(target: "kad-client", %key, ?query_id, "lookup started");
        self.pending.insert(
            query_id,
            PendingQuery {
                key,
                best: None,
                copies_returned: 0,
                deadline: Instant::now() + self.query_timeout,
                reply,
            },
        );
    }

    /// Resolve every lookup whose client-side deadline has passed and abort its kademlia query.
    fn expire_lookups(&mut self) {
        let now = Instant::now();
        let expired: Vec<kad::QueryId> = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            // finishing the query makes kademlia emit a terminal event for it, which is ignored
            // because the pending entry is gone by then
            if let Some(mut query) = self.swarm.behaviour_mut().kademlia.query_mut(&id) {
                query.finish();
            }
            self.finish_lookup(id, Terminal::Timeout);
        }
    }

    /// Deliver the outcome of a lookup and forget it.
    ///
    /// Any valid copy wins regardless of how the lookup ended, mirroring the node: a `NotFound`
    /// after a valid copy was already collected is a success. With no valid copy, the outcome
    /// depends on what the DHT reported: copies that all failed verification, no peer answering
    /// on the protocol at all, a timeout, or a clean miss.
    fn finish_lookup(&mut self, id: kad::QueryId, terminal: Terminal) {
        let Some(pending) = self.pending.remove(&id) else {
            trace!(target: "kad-client", ?id, "terminal event for a lookup no longer tracked");
            return;
        };
        let outcome = match (pending.best, terminal) {
            (Some(best), _) => Ok(Some(best)),
            (None, _) if pending.copies_returned > 0 => {
                Err(KadClientError::InvalidRecords { copies: pending.copies_returned })
            }
            (None, Terminal::Timeout) => Err(KadClientError::Timeout),
            (None, Terminal::Finished(stats)) if stats.num_successes() == 0 => {
                Err(KadClientError::NoPeerAnswered {
                    protocol: self.protocol.clone(),
                    requests: stats.num_requests(),
                })
            }
            (None, Terminal::Finished(_)) => Ok(None),
        };
        debug!(target: "kad-client", key = %pending.key, ?id, ?outcome, "lookup finished");
        // the caller may have gone away; nothing to do about it
        let _ = pending.reply.send(outcome);
    }

    /// Verify a returned copy and fold it into the lookup's best record.
    fn accept_copy(&mut self, id: kad::QueryId, record: &kad::Record, from: Option<PeerId>) {
        let Some(pending) = self.pending.get_mut(&id) else {
            trace!(target: "kad-client", ?id, "record for a lookup no longer tracked");
            return;
        };
        pending.copies_returned += 1;
        match verify_record(self.domain, &pending.key, record) {
            Ok(verified) => fold_newest(&mut pending.best, verified),
            Err(reason) => {
                debug!(target: "kad-client", key = %pending.key, ?from, ?reason, "discarding record copy")
            }
        }
    }

    /// Route a swarm event.
    fn handle_swarm_event(&mut self, event: SwarmEvent<ClientBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(ClientBehaviourEvent::Kademlia(event)) => {
                self.handle_kad_event(event)
            }
            SwarmEvent::Behaviour(ClientBehaviourEvent::Gossipsub(event)) => {
                self.handle_gossip_event(event)
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, num_established, .. } => {
                debug!(
                    target: "kad-client",
                    peer = %peer_id,
                    address = %endpoint.get_remote_address(),
                    connections = num_established,
                    "connection established"
                );
                if let Some(connections) = self.bootstrap.get_mut(&peer_id) {
                    *connections = num_established.get();
                    self.dial_failed.remove(&peer_id);
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, num_established, cause, .. } => {
                debug!(
                    target: "kad-client",
                    peer = %peer_id,
                    remaining = num_established,
                    ?cause,
                    "connection closed"
                );
                if let Some(connections) = self.bootstrap.get_mut(&peer_id) {
                    *connections = num_established;
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                debug!(target: "kad-client", peer = ?peer_id, %error, "outgoing connection failed");
                if let Some(peer) = peer_id {
                    if self.bootstrap.contains_key(&peer) {
                        self.dial_failed.insert(peer);
                    }
                }
            }
            other => trace!(target: "kad-client", event = ?other, "swarm event"),
        }
    }

    /// Route a kademlia event; only `GET_VALUE` progress is acted on.
    fn handle_kad_event(&mut self, event: kad::Event) {
        match event {
            kad::Event::OutboundQueryProgressed {
                id,
                result: kad::QueryResult::GetRecord(result),
                step,
                stats,
            } => self.handle_get_record(id, result, step, stats),
            other => trace!(target: "kad-client", event = ?other, "kad event"),
        }
    }

    /// Drop anything gossipsub hands us.
    ///
    /// The client subscribes to no topic, so a message can only arrive if a peer pushes one
    /// regardless; it is marked ignored so gossipsub neither forwards it nor scores the sender.
    fn handle_gossip_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message { propagation_source, message_id, message } => {
                trace!(target: "kad-client", peer = %propagation_source, topic = %message.topic, "dropping unsolicited gossip");
                let _ = self.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    gossipsub::MessageAcceptance::Ignore,
                );
            }
            other => trace!(target: "kad-client", event = ?other, "gossipsub event"),
        }
    }

    /// Fold one `GET_VALUE` progress event into its lookup.
    ///
    /// A lookup emits zero or more `FoundRecord` events and then exactly one terminal event:
    /// `FinishedWithNoAdditionalRecord` once a record was found, or an error otherwise. A
    /// `FoundRecord` carrying `step.last` is itself terminal, as in the node's event arm.
    fn handle_get_record(
        &mut self,
        id: kad::QueryId,
        result: kad::GetRecordResult,
        step: kad::ProgressStep,
        stats: kad::QueryStats,
    ) {
        match result {
            Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord { record, peer })) => {
                self.accept_copy(id, &record, peer);
                if step.last {
                    self.finish_lookup(id, Terminal::Finished(stats));
                }
            }
            Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. }) => {
                self.finish_lookup(id, Terminal::Finished(stats));
            }
            Err(kad::GetRecordError::NotFound { .. }) => {
                self.finish_lookup(id, Terminal::Finished(stats));
            }
            Err(kad::GetRecordError::Timeout { .. }) => {
                self.finish_lookup(id, Terminal::Timeout);
            }
        }
    }
}

/// Build the topic-less gossipsub instance that keeps the node from banning the client.
///
/// Mirrors the node's builder in `ConsensusNetwork::new`: strict validation, application-side
/// validation of every message (so nothing is ever forwarded without an explicit accept, which
/// the client never gives), and the chain-namespaced protocol id prefix the node negotiates on.
fn build_gossipsub(
    keypair: &identity::Keypair,
    chain_id: u64,
) -> Result<gossipsub::Behaviour, KadClientError> {
    let config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Strict)
        .validate_messages()
        .protocol_id_prefix(gossip_protocol_id_prefix(chain_id))
        .build()
        .map_err(|e| KadClientError::Transport(format!("invalid gossipsub config: {e}")))?;
    gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Signed(keypair.clone()), config)
        .map_err(|e| KadClientError::Transport(format!("failed to build gossipsub: {e}")))
}

/// Extract the peer id a bootstrap address must end with and confirm the address is one the
/// QUIC-only transport can dial.
fn parse_bootstrap_addr(addr: &Multiaddr) -> Result<PeerId, KadClientError> {
    let reject = |reason: &str| KadClientError::InvalidBootstrapAddr {
        addr: addr.clone(),
        reason: reason.to_string(),
    };
    let peer = match addr.iter().last() {
        Some(Protocol::P2p(peer)) => peer,
        _ => return Err(reject("missing trailing /p2p/<peer-id> component")),
    };
    if !addr.iter().any(|protocol| matches!(protocol, Protocol::QuicV1)) {
        return Err(reject("the client dials QUIC only; expected /udp/<port>/quic-v1"));
    }
    Ok(peer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_addr_requires_p2p_and_quic() {
        let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        let good: Multiaddr =
            format!("/ip4/127.0.0.1/udp/49594/quic-v1/p2p/{peer}").parse().expect("parses");
        assert_eq!(parse_bootstrap_addr(&good), Ok(peer));

        let no_peer: Multiaddr = "/ip4/127.0.0.1/udp/49594/quic-v1".parse().expect("parses");
        assert!(matches!(
            parse_bootstrap_addr(&no_peer),
            Err(KadClientError::InvalidBootstrapAddr { .. })
        ));

        let tcp: Multiaddr =
            format!("/ip4/127.0.0.1/tcp/49594/p2p/{peer}").parse().expect("parses");
        assert!(matches!(
            parse_bootstrap_addr(&tcp),
            Err(KadClientError::InvalidBootstrapAddr { .. })
        ));
    }

    #[test]
    fn build_rejects_empty_and_malformed_bootstrap() {
        let empty = KadClientConfig::new(2017, tn_node_record::NetworkType::Worker(0), vec![]);
        assert!(matches!(Driver::build(&empty), Err(KadClientError::NoBootstrapPeers)));

        let bad: Multiaddr = "/ip4/127.0.0.1/udp/49594/quic-v1".parse().expect("parses");
        let malformed =
            KadClientConfig::new(2017, tn_node_record::NetworkType::Worker(0), vec![bad]);
        assert!(matches!(
            Driver::build(&malformed),
            Err(KadClientError::InvalidBootstrapAddr { .. })
        ));
    }
}
