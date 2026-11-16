//! A committee lookup that joins a tracked kad query in flight is never lost: when that query
//! closes without a record, the key goes back to the peer manager, which re-issues the committee
//! lookup while the key's record is still unlearned.
//!
//! The per-query bookkeeping is private to the swarm, so these tests sit beside it. The swarm is
//! never polled, so a query ends only when a test closes it; the peer manager's queued lookups are
//! handed to the swarm handler exactly as a swarm poll would hand them.

use super::*;
use crate::common::{TestWorkerRequest, TestWorkerResponse};
use eyre::eyre;
use libp2p::swarm::ToSwarm;
use rand::{rngs::StdRng, SeedableRng as _};
use std::task::{Context, Poll, Waker};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsKeypair, NetworkPublicKey, Signer as _, TaskManager};

/// The events a test swarm reports to the application.
type TestEvent = NetworkEvent<TestWorkerRequest, TestWorkerResponse>;

/// The swarm under test.
type TestNetwork =
    ConsensusNetwork<TestWorkerRequest, TestWorkerResponse, MemDatabase, Sender<TestEvent>>;

/// A deterministic BLS key for `seed`.
fn bls(seed: u8) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
}

/// A QUIC endpoint an authenticated reader accepts.
fn quic_address() -> eyre::Result<Multiaddr> {
    Ok("/ip4/127.0.0.1/udp/8000/quic-v1".parse()?)
}

/// One query in flight: its id, class, and whether a committee lookup joined it.
type InFlight = (QueryId, LookupClass, bool);

/// A primary swarm, with the event receiver and task manager it needs kept alive.
struct Fixture {
    /// The swarm under test.
    network: TestNetwork,
    /// Keeps the application event channel open.
    _events: Receiver<TestEvent>,
    /// Owns the tasks the swarm spawned.
    _tasks: TaskManager,
}

impl Fixture {
    /// Build the first fixture authority's primary swarm.
    fn new() -> eyre::Result<Self> {
        let committee = CommitteeFixture::builder(MemDatabase::default).build();
        let config = committee
            .authorities()
            .next()
            .ok_or_else(|| eyre!("fixture has an authority"))?
            .consensus_config();
        let (sender, events) = tokio::sync::mpsc::channel(10);
        let tasks = TaskManager::default();
        let network = ConsensusNetwork::new(
            config.network_config(),
            sender,
            config.key_config().clone(),
            config.key_config().primary_network_keypair().clone(),
            MemDatabase::default(),
            tasks.get_spawner(),
            NetworkType::Primary,
            config.primary_address(),
            None,
        )?;
        Ok(Self { network, _events: events, _tasks: tasks })
    }

    /// The record queries in flight for `key`.
    fn queries(&self, key: &BlsPublicKey) -> Vec<InFlight> {
        self.network
            .kad_record_queries
            .iter()
            .filter(|(_, query)| query.query.request == *key)
            .map(|(id, query)| (*id, query.class, query.rearm_committee))
            .collect()
    }

    /// The id of the single fresh query in flight for `key`, which must be of `class`.
    fn fresh_query(&self, key: &BlsPublicKey, class: LookupClass) -> eyre::Result<QueryId> {
        match self.queries(key).as_slice() {
            [(id, held, false)] if *held == class => Ok(*id),
            other => Err(eyre!("expected one fresh {class:?} query, found {other:?}")),
        }
    }

    /// Hand every lookup the peer manager has queued to the swarm handler, and return the keys
    /// requested as committee lookups.
    fn forward_lookups(&mut self) -> eyre::Result<Vec<BlsPublicKey>> {
        let mut context = Context::from_waker(Waker::noop());
        let mut committee = Vec::new();
        while let Poll::Ready(ToSwarm::GenerateEvent(event)) = NetworkBehaviour::poll(
            &mut self.network.swarm.behaviour_mut().peer_manager,
            &mut context,
        ) {
            if let PeerEvent::MissingAuthorities(keys) = &event {
                committee.extend(keys.iter().copied());
            }
            if matches!(event, PeerEvent::MissingAuthorities(_) | PeerEvent::MissingRecords(..)) {
                self.network.process_peer_manager_event(event)?;
            }
        }
        Ok(committee)
    }

    /// Issue a tracked lookup for `key` directly and return its query id.
    fn track(&mut self, key: BlsPublicKey) -> eyre::Result<QueryId> {
        self.network.process_peer_manager_event(PeerEvent::MissingRecords(
            vec![key],
            LookupClass::Tracked,
        ))?;
        self.fresh_query(&key, LookupClass::Tracked)
    }

    /// Put `keys` in the current committee and forward the lookups that queues.
    fn enter_window(&mut self, keys: HashSet<BlsPublicKey>) -> eyre::Result<HashSet<BlsPublicKey>> {
        self.network.swarm.behaviour_mut().peer_manager.update_committees(
            HashSet::new(),
            keys,
            HashSet::new(),
        );
        Ok(self.forward_lookups()?.into_iter().collect())
    }
}

/// A key the scheduler queried as tracked enters the window: its committee lookup joins the
/// tracked query, and the query closing empty re-issues the committee lookup at once.
#[tokio::test]
async fn tracked_query_closing_empty_rearms_joined_committee_lookup() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let key = bls(1);
    fixture.network.swarm.behaviour_mut().peer_manager.update_tracked_keys(
        1,
        HashSet::new(),
        HashSet::from([key]),
    );
    // the first heartbeat is due at construction, but the timer only fires once the runtime has
    // turned its time driver
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(fixture.forward_lookups()?.is_empty(), "the heartbeat asks for a tracked lookup only");
    let tracked = fixture.fresh_query(&key, LookupClass::Tracked)?;

    assert_eq!(fixture.enter_window(HashSet::from([key]))?, HashSet::from([key]));
    assert_eq!(
        fixture.queries(&key),
        [(tracked, LookupClass::Tracked, true)],
        "the committee lookup joins the tracked query instead of waiting or duplicating it"
    );

    fixture.network.close_kad_query(&tracked);
    assert!(fixture.network.kad_record_queries.is_empty());
    assert_eq!(fixture.forward_lookups()?, [key], "the empty close re-arms the committee lookup");
    let rearmed = fixture.fresh_query(&key, LookupClass::Committee)?;
    assert_ne!(rearmed, tracked, "the re-armed lookup is a fresh kad query");
    Ok(())
}

/// A tracked query that only tracked lookups joined closes empty without issuing anything.
#[tokio::test]
async fn tracked_query_without_committee_join_is_not_rearmed() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let key = bls(2);
    let tracked = fixture.track(key)?;
    fixture
        .network
        .process_peer_manager_event(PeerEvent::MissingRecords(vec![key], LookupClass::Tracked))?;
    assert_eq!(fixture.queries(&key), [(tracked, LookupClass::Tracked, false)]);

    fixture.network.close_kad_query(&tracked);
    assert!(!fixture.forward_lookups()?.contains(&key));
    assert!(fixture.network.kad_record_queries.is_empty());
    Ok(())
}

/// A committee query is the committee lookup itself: a tracked or a second committee lookup
/// joining it does not mark it, and its empty close issues nothing.
#[tokio::test]
async fn committee_query_joined_by_other_lookups_is_not_marked() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let key = bls(3);
    fixture.network.process_peer_manager_event(PeerEvent::MissingAuthorities(vec![key]))?;
    let committee = fixture.fresh_query(&key, LookupClass::Committee)?;
    fixture
        .network
        .process_peer_manager_event(PeerEvent::MissingRecords(vec![key], LookupClass::Tracked))?;
    fixture.network.process_peer_manager_event(PeerEvent::MissingAuthorities(vec![key]))?;
    assert_eq!(fixture.queries(&key), [(committee, LookupClass::Committee, false)]);

    fixture.network.close_kad_query(&committee);
    assert!(!fixture.forward_lookups()?.contains(&key));
    assert!(fixture.network.kad_record_queries.is_empty());
    Ok(())
}

/// A joined tracked query that finds a record resolves the new window member through the
/// committee cache, since the result routes by membership at close, and re-issues nothing.
#[tokio::test]
async fn joined_tracked_query_with_record_resolves_member() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let keypair = BlsKeypair::generate(&mut StdRng::from_seed([4; 32]));
    let key = *keypair.public();
    let tracked = fixture.track(key)?;
    assert_eq!(fixture.enter_window(HashSet::from([key]))?, HashSet::from([key]));
    assert_eq!(fixture.queries(&key), [(tracked, LookupClass::Tracked, true)]);

    let network_keypair = NetworkKeypair::generate_ed25519();
    let record = NodeRecord::build(
        RecordDomain::new(911_329, NetworkType::Primary),
        &key,
        &network_keypair,
        quic_address()?,
        None,
        |bytes| keypair.sign(bytes),
    )?;
    let peer: PeerId = record.info.pubkey.clone().into();
    fixture
        .network
        .kad_record_queries
        .get_mut(&tracked)
        .ok_or_else(|| eyre!("tracked query in flight"))?
        .consider(record, now());
    fixture.network.close_kad_query(&tracked);

    let manager = &fixture.network.swarm.behaviour().peer_manager;
    assert_eq!(manager.auth_to_peer(key).map(|(id, _)| id), Some(peer), "member resolved");
    assert_eq!(manager.peer_to_bls(&peer), Some(key));
    assert!(!fixture.forward_lookups()?.contains(&key));
    assert!(fixture.network.kad_record_queries.is_empty(), "a found record re-arms nothing");
    Ok(())
}

/// The re-arm uses the peer manager's own test of a still unlearned record: a configured stub
/// does not count as resolved and is looked up again, while a key whose signed record arrived by
/// another path while its tracked query was in flight is not.
#[tokio::test]
async fn rearm_treats_a_configured_stub_as_unlearned() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let (stub, learned) = (bls(5), bls(6));
    let configured = |pubkey: NetworkPublicKey| -> eyre::Result<NetworkInfo> {
        Ok(NetworkInfo { pubkey, multiaddrs: vec![quic_address()?], timestamp: now(), rpc: None })
    };
    fixture
        .network
        .swarm
        .behaviour_mut()
        .peer_manager
        .add_bootstrap_peer(stub, configured(NetworkKeypair::generate_ed25519().public().into())?);
    let (stub_query, learned_query) = (fixture.track(stub)?, fixture.track(learned)?);
    assert_eq!(
        fixture.enter_window(HashSet::from([stub, learned]))?,
        HashSet::from([stub, learned])
    );
    assert_eq!(fixture.queries(&stub), [(stub_query, LookupClass::Tracked, true)]);
    assert_eq!(fixture.queries(&learned), [(learned_query, LookupClass::Tracked, true)]);

    let record = configured(NetworkKeypair::generate_ed25519().public().into())?;
    fixture.network.swarm.behaviour_mut().peer_manager.add_discovered_peer(learned, record);
    fixture.network.close_kad_query(&stub_query);
    fixture.network.close_kad_query(&learned_query);

    assert_eq!(fixture.forward_lookups()?, [stub], "only the stub is still unlearned");
    fixture.fresh_query(&stub, LookupClass::Committee)?;
    assert!(fixture.queries(&learned).is_empty(), "a learned record is not looked up again");
    Ok(())
}
