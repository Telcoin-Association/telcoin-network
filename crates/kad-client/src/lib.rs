//! A read-only client for one Telcoin chain and role's node-record DHT.
//!
//! The client has an ephemeral identity and no listener. Kademlia runs in client mode,
//! filters inbound writes and disables write-back caching and publication. A topic-less
//! gossipsub behaviour negotiates the chain's gossip protocol so validators do not penalize
//! the connection for lacking gossipsub support.

mod error;

pub use error::Error;

use futures::{future, StreamExt, TryStreamExt};
use libp2p::{
    gossipsub,
    identity::Keypair,
    kad::{self, store::MemoryStore, GetRecordError, GetRecordOk, QueryResult},
    multiaddr::Protocol,
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::{fmt, time::Duration};
use tn_node_record::{
    gossip_protocol_id_prefix, NetworkType, NodeRecord, RecordDomain, MAX_ADVERTISED_MULTIADDRS,
};
use tn_types::BlsPublicKey;

/// Protocols required to query records without triggering a node penalty.
#[derive(NetworkBehaviour)]
struct Behaviour {
    /// Client-mode DHT with no accepted inbound records.
    kad: kad::Behaviour<MemoryStore>,
    /// Protocol negotiation only; the reader never subscribes or publishes.
    gossip: gossipsub::Behaviour,
}

/// A reader for one `(chain_id, NetworkType)` DHT.
///
/// Bootstrap addresses use `/ip4/ADDRESS/udp/PORT/quic-v1/p2p/PEER_ID`.
/// IPv6 and DNS hosts are also accepted. At least one usable address is required.
/// Lookups run sequentially; use separate clients for independent DHTs.
pub struct Client {
    /// Private swarm so callers cannot add listeners or publish records.
    swarm: Swarm<Behaviour>,
    /// Locally reconstructed signature domain.
    domain: RecordDomain,
    /// Maximum duration of a lookup, including connection establishment.
    query_timeout: Duration,
    /// Addresses retained so later lookups can retry disconnected bootstrap peers.
    bootstrap: Vec<(PeerId, Multiaddr)>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("peer_id", self.swarm.local_peer_id())
            .field("domain", &self.domain)
            .field("query_timeout", &self.query_timeout)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Construct a reader with a fresh identity and a positive lookup deadline.
    ///
    /// Unusable bootstrap entries are ignored if at least one usable entry remains.
    /// Construction does not listen or perform a lookup.
    pub fn new(
        chain_id: u64,
        network: NetworkType,
        bootstrap: impl IntoIterator<Item = Multiaddr>,
        query_timeout: Duration,
    ) -> Result<Self, Error> {
        let bootstrap: Vec<_> = bootstrap.into_iter().filter_map(bootstrap_peer).collect();
        (!bootstrap.is_empty()).then_some(()).ok_or(Error::InvalidBootstrap)?;
        (!query_timeout.is_zero())
            .then_some(())
            .ok_or_else(|| Error::Configuration("query timeout must be positive".into()))?;
        let identity = Keypair::generate_ed25519();
        let peer_id = identity.public().to_peer_id();
        let protocol = StreamProtocol::try_from_owned(network.kad_protocol_name(chain_id))
            .map_err(|error| Error::Configuration(error.to_string()))?;
        let mut config = kad::Config::new(protocol);
        config
            .set_query_timeout(query_timeout)
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            .set_caching(kad::Caching::Disabled)
            .set_publication_interval(None)
            .set_replication_interval(None)
            .set_provider_publication_interval(None)
            .set_periodic_bootstrap_interval(None);
        let mut kad = kad::Behaviour::with_config(peer_id, MemoryStore::new(peer_id), config);
        kad.set_mode(Some(kad::Mode::Client));
        let gossip_config = gossipsub::ConfigBuilder::default()
            .protocol_id_prefix(gossip_protocol_id_prefix(chain_id))
            .build()
            .map_err(|error| Error::Configuration(error.to_string()))?;
        let gossip = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(identity.clone()),
            gossip_config,
        )
        .map_err(|error| Error::Configuration(error.to_string()))?;
        let behaviour = Behaviour { kad, gossip };
        let swarm = SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_quic()
            .with_dns()
            .map_err(Error::Transport)?
            .with_behaviour(|_| behaviour)
            .map_err(|error| Error::Configuration(error.to_string()))?
            .with_swarm_config(|config| config.with_idle_connection_timeout(query_timeout))
            .build();
        Ok(Self { swarm, domain: RecordDomain::new(chain_id, network), query_timeout, bootstrap })
    }

    /// Return the newest verified copy, or `None` after a successful lookup with no copies.
    ///
    /// Copies must match the requested raw BLS key, verify under the configured domain,
    /// fit the shared address cap, and name the signed network identity as publisher.
    /// Equal timestamps keep the first verified copy, matching the node's freshness policy.
    /// A verified copy remains usable if another peer times out later in the query.
    pub async fn lookup(&mut self, key: &BlsPublicKey) -> Result<Option<NodeRecord>, Error> {
        self.bootstrap.iter().for_each(|(peer, address)| {
            self.swarm.behaviour_mut().kad.add_address(peer, address.clone());
        });
        let query = self.swarm.behaviour_mut().kad.get_record(kad::RecordKey::new(&key.as_ref()));
        let mut lookup =
            Lookup { connected: self.swarm.network_info().num_peers() > 0, ..Lookup::default() };
        let domain = self.domain;
        let events = self
            .swarm
            .by_ref()
            .map(Ok)
            .try_for_each(|event| future::ready(lookup.on_event(event, query, key, domain)));
        let completion = tokio::time::timeout(self.query_timeout, events)
            .await
            .map_or(Completion::Timeout, |result| result.err().unwrap_or(Completion::Finished));
        self.swarm
            .behaviour_mut()
            .kad
            .query_mut(&query)
            .into_iter()
            .for_each(|mut query| query.finish());
        lookup.finish(completion)
    }
}

/// How a query stopped independently of whether it returned verified copies.
#[derive(Clone, Copy)]
enum Completion {
    /// The DHT exhausted its lookup candidates.
    Finished,
    /// The local or DHT deadline expired.
    Timeout,
}

/// Validation and newest-wins folding for one query's untrusted responses.
#[derive(Default)]
struct Lookup {
    /// Newest verified copy seen so far.
    newest: Option<NodeRecord>,
    /// Whether any peer returned a copy, including invalid copies.
    saw_copies: bool,
    /// Whether a bootstrap connection was established.
    connected: bool,
    /// Successful protocol requests reported by the Kademlia query.
    successful_requests: u32,
}

impl Lookup {
    /// Consume only the requested query while driving both network protocols.
    fn on_event(
        &mut self,
        event: SwarmEvent<BehaviourEvent>,
        query: kad::QueryId,
        key: &BlsPublicKey,
        domain: RecordDomain,
    ) -> Result<(), Completion> {
        if let SwarmEvent::ConnectionEstablished { .. } = &event {
            self.connected = true;
        }
        if let SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
            id,
            result: QueryResult::GetRecord(result),
            stats,
            ..
        })) = event
        {
            (id == query).then_some((result, stats)).map_or(Ok(()), |(result, stats)| {
                self.successful_requests = stats.num_successes();
                result.map_or_else(
                    |error| {
                        Err(match error {
                            GetRecordError::NotFound { .. } => Completion::Finished,
                            GetRecordError::Timeout { .. } => Completion::Timeout,
                        })
                    },
                    |success| match success {
                        GetRecordOk::FoundRecord(record) => {
                            self.observe(&record.record, key, domain);
                            Ok(())
                        }
                        GetRecordOk::FinishedWithNoAdditionalRecord { .. } => {
                            Err(Completion::Finished)
                        }
                    },
                )
            })
        } else {
            Ok(())
        }
    }

    /// Admit a copy only after all node-equivalent checks succeed.
    fn observe(&mut self, record: &kad::Record, key: &BlsPublicKey, domain: RecordDomain) {
        self.saw_copies = true;
        let verified = (record.key.as_ref() == key.as_ref())
            .then(|| NodeRecord::decode_and_verify(&record.value, domain, key))
            .flatten()
            .map(|(_, record)| record)
            .filter(|node| node.info.multiaddrs.len() <= MAX_ADVERTISED_MULTIADDRS)
            .filter(|node| record.publisher == Some(node.info.pubkey.clone().into()));
        self.newest = verified
            .filter(|node| {
                self.newest.as_ref().is_none_or(|old| node.info.timestamp > old.info.timestamp)
            })
            .or_else(|| self.newest.take());
    }

    /// Keep validation failures distinct from a successful empty response.
    fn finish(self, completion: Completion) -> Result<Option<NodeRecord>, Error> {
        self.newest.map(Some).map_or_else(
            || {
                if self.saw_copies {
                    Err(Error::NoVerifiedRecords)
                } else {
                    match completion {
                        Completion::Timeout => Err(Error::Timeout),
                        Completion::Finished if self.successful_requests > 0 => Ok(None),
                        Completion::Finished if self.connected => Err(Error::NoCompatiblePeers),
                        Completion::Finished => Err(Error::BootstrapUnavailable),
                    }
                }
            },
            Ok,
        )
    }
}

/// Split a dialable QUIC address from its required terminal peer identity.
fn bootstrap_peer(mut address: Multiaddr) -> Option<(PeerId, Multiaddr)> {
    let parts: Vec<_> = address.iter().collect();
    if let [Protocol::Ip4(_)
    | Protocol::Ip6(_)
    | Protocol::Dns(_)
    | Protocol::Dns4(_)
    | Protocol::Dns6(_), Protocol::Udp(port), Protocol::QuicV1, Protocol::P2p(peer)] =
        parts.as_slice()
    {
        let peer = *peer;
        let usable = *port > 0;
        drop(parts);
        usable.then(|| {
            address.pop();
            (peer, address)
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
