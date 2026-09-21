//! Validation, outcome classification and local QUIC integration tests.

use super::*;
use libp2p::kad::store::RecordStore;
use rand::{rngs::StdRng, SeedableRng};
use std::net::Ipv4Addr;
use tn_types::{encode, BlsKeypair, NetworkPublicKey, Signer, WorkerId};

/// Fixed timeout bounds network tests without asserting elapsed wall-clock time.
const TIMEOUT: Duration = Duration::from_secs(10);

/// A localhost QUIC endpoint. Port zero is used only by test listeners.
fn endpoint(port: u16) -> Multiaddr {
    Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::LOCALHOST))
        .with(Protocol::Udp(port))
        .with(Protocol::QuicV1)
}

/// Reproducible BLS signing key for a fixture.
fn signer(seed: u64) -> BlsKeypair {
    BlsKeypair::generate(&mut StdRng::seed_from_u64(seed))
}

/// Build a signed record with explicit freshness and address count for boundary tests.
fn signed_record(
    key: &BlsKeypair,
    chain: u64,
    network: NetworkType,
    timestamp: u64,
    addresses: usize,
) -> kad::Record {
    let identity = Keypair::generate_ed25519();
    let network_key: NetworkPublicKey = identity.public().into();
    let mut node = NodeRecord::build(
        RecordDomain::new(chain, network),
        network_key,
        endpoint(9000),
        None,
        |bytes| key.sign(bytes),
    );
    node.info.timestamp = timestamp;
    node.info.multiaddrs = (0..addresses).map(|_| endpoint(9000)).collect();
    let (role, worker): (u8, WorkerId) = match network {
        NetworkType::Primary => (0, 0),
        NetworkType::Worker(id) => (1, id),
    };
    node.signature = key.sign(&encode(&(
        b"telcoin-network/node-record/v1".as_slice(),
        chain,
        role,
        worker,
        &node.info,
    )));
    kad::Record {
        key: kad::RecordKey::new(&key.public().as_ref()),
        value: encode(&node),
        publisher: Some(identity.public().to_peer_id()),
        expires: None,
    }
}

/// Valid copies verify with exactly the same domain and publisher rules as a node.
#[test]
fn accepts_valid_record_and_distinguishes_invalid_copies() -> Result<(), Error> {
    let key = signer(1);
    let valid = signed_record(&key, 2017, NetworkType::Worker(0), 10, MAX_ADVERTISED_MULTIADDRS);
    let domain = RecordDomain::new(2017, NetworkType::Worker(0));
    let mut lookup = Lookup::default();
    lookup.observe(&valid, key.public(), domain);
    let record = lookup.finish(Completion::Finished)?.ok_or(Error::NoVerifiedRecords)?;
    assert_eq!(record.info.timestamp, 10);

    let mut wrong_publisher = valid.clone();
    wrong_publisher.publisher = Some(Keypair::generate_ed25519().public().to_peer_id());
    let mut missing_publisher = valid.clone();
    missing_publisher.publisher = None;
    let mut wrong_key = valid.clone();
    wrong_key.key = kad::RecordKey::new(&signer(2).public().as_ref());
    let mut malformed = valid.clone();
    malformed.value = vec![0];
    let mut tampered = valid.clone();
    tampered.value.last_mut().into_iter().for_each(|byte| *byte ^= 1);
    let cases = [
        wrong_publisher,
        missing_publisher,
        wrong_key,
        malformed,
        tampered,
        signed_record(&key, 2018, NetworkType::Worker(0), 10, 1),
        signed_record(&key, 2017, NetworkType::Primary, 10, 1),
        signed_record(&key, 2017, NetworkType::Worker(1), 10, 1),
        signed_record(&key, 2017, NetworkType::Worker(0), 10, MAX_ADVERTISED_MULTIADDRS + 1),
    ];
    cases.iter().for_each(|record| {
        let mut lookup = Lookup::default();
        lookup.observe(record, key.public(), domain);
        assert!(matches!(lookup.finish(Completion::Finished), Err(Error::NoVerifiedRecords)));
    });
    Ok(())
}

/// The newest verified copy wins; older, equal-timestamp and invalid copies cannot replace it.
#[test]
fn folds_copies_newest_wins_even_on_timeout() -> Result<(), Error> {
    let key = signer(3);
    let newest = signed_record(&key, 2017, NetworkType::Primary, 20, 1);
    let older = signed_record(&key, 2017, NetworkType::Primary, 10, 1);
    let equal = signed_record(&key, 2017, NetworkType::Primary, 20, 1);
    let invalid = signed_record(&key, 2018, NetworkType::Primary, 30, 1);
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let mut lookup = Lookup::default();
    [&older, &newest, &older, &equal, &invalid]
        .into_iter()
        .for_each(|record| lookup.observe(record, key.public(), domain));
    lookup.finish(Completion::Timeout)?.ok_or(Error::NoVerifiedRecords).map(|record| {
        assert_eq!(encode(&record), newest.value);
    })
}

/// Clean misses, unavailable peers, protocol mismatches and deadlines remain distinct.
#[test]
fn classifies_lookup_outcomes() {
    assert!(matches!(
        Lookup::default().finish(Completion::Finished),
        Err(Error::BootstrapUnavailable)
    ));
    assert!(matches!(
        Lookup { connected: true, ..Lookup::default() }.finish(Completion::Finished),
        Err(Error::NoCompatiblePeers)
    ));
    assert!(matches!(
        Lookup { successful_requests: 1, ..Lookup::default() }.finish(Completion::Finished),
        Ok(None)
    ));
    assert!(matches!(Lookup::default().finish(Completion::Timeout), Err(Error::Timeout)));
    assert!(matches!(
        Lookup { saw_copies: true, ..Lookup::default() }.finish(Completion::Timeout),
        Err(Error::NoVerifiedRecords)
    ));
}

/// Invalid bootstrap configuration is rejected before a network operation is attempted.
#[tokio::test]
async fn requires_quic_and_peer_identity() -> Result<(), Error> {
    let peer = Keypair::generate_ed25519().public().to_peer_id();
    assert!(matches!(
        Client::new(2017, NetworkType::Primary, [], TIMEOUT),
        Err(Error::InvalidBootstrap)
    ));
    assert!(bootstrap_peer(endpoint(9000)).is_none());
    assert!(bootstrap_peer(endpoint(0).with(Protocol::P2p(peer))).is_none());
    let address = endpoint(9000).with(Protocol::P2p(peer));
    assert_eq!(bootstrap_peer(address.clone()), Some((peer, endpoint(9000))));
    let mut client = Client::new(2017, NetworkType::Primary, [address.clone()], TIMEOUT)?;
    let other = Client::new(2017, NetworkType::Primary, [address], TIMEOUT)?;
    assert_ne!(client.swarm.local_peer_id(), other.swarm.local_peer_id());
    assert_eq!(client.swarm.listeners().count(), 0);
    assert_eq!(client.swarm.behaviour().kad.mode(), kad::Mode::Client);
    assert_eq!(client.swarm.behaviour().gossip.topics().count(), 0);
    assert_eq!(client.swarm.behaviour_mut().kad.store_mut().records().count(), 0);
    Ok(())
}

/// Running local DHT server, aborted automatically on every test exit path.
struct Server {
    /// Bound QUIC address with the server peer ID.
    address: Multiaddr,
    /// Event driver that keeps the server responding.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start a real QUIC DHT server, binding port zero without a close-and-rebind race.
async fn server(network: NetworkType, records: Vec<kad::Record>) -> Result<Server, Error> {
    let placeholder =
        endpoint(1).with(Protocol::P2p(Keypair::generate_ed25519().public().to_peer_id()));
    let mut reader = Client::new(2017, network, [placeholder], TIMEOUT)?;
    reader.swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server));
    records.into_iter().try_for_each(|record| {
        reader
            .swarm
            .behaviour_mut()
            .kad
            .store_mut()
            .put(record)
            .map_err(|error| Error::Configuration(error.to_string()))
    })?;
    reader.swarm.listen_on(endpoint(0)).map_err(|error| Error::Configuration(error.to_string()))?;
    let peer = *reader.swarm.local_peer_id();
    let address = reader
        .swarm
        .by_ref()
        .filter_map(|event| {
            future::ready(if let SwarmEvent::NewListenAddr { address, .. } = event {
                Some(address)
            } else {
                None
            })
        })
        .next()
        .await
        .ok_or(Error::BootstrapUnavailable)?
        .with(Protocol::P2p(peer));
    let task = tokio::spawn(reader.swarm.for_each(|_event| future::ready(())));
    Ok(Server { address, task })
}

/// Real QUIC lookups fold copies across peers and leave the reader's store empty.
#[tokio::test]
async fn quic_lookup_returns_newest_verified_record() -> Result<(), Error> {
    let key = signer(4);
    let older = signed_record(&key, 2017, NetworkType::Worker(0), 10, 1);
    let newest = signed_record(&key, 2017, NetworkType::Worker(0), 20, 1);
    let first = server(NetworkType::Worker(0), vec![older]).await?;
    let second = server(NetworkType::Worker(0), vec![newest.clone()]).await?;
    let mut client = Client::new(
        2017,
        NetworkType::Worker(0),
        [first.address.clone(), second.address.clone()],
        TIMEOUT,
    )?;
    let found = client.lookup(key.public()).await?.ok_or(Error::NoVerifiedRecords)?;
    assert_eq!(encode(&found), newest.value);
    assert_eq!(client.swarm.listeners().count(), 0);
    assert_eq!(client.swarm.behaviour_mut().kad.store_mut().records().count(), 0);
    assert_eq!(client.swarm.behaviour().gossip.topics().count(), 0);
    Ok(())
}

/// A successful empty DHT response is a clean miss, not a connectivity failure.
#[tokio::test]
async fn quic_lookup_reports_clean_miss() -> Result<(), Error> {
    let server = server(NetworkType::Primary, Vec::new()).await?;
    let mut client = Client::new(2017, NetworkType::Primary, [server.address.clone()], TIMEOUT)?;
    client.lookup(signer(5).public()).await.map(|record| assert!(record.is_none()))
}

/// A connected peer speaking another role's DHT protocol is not reported as a clean miss.
#[tokio::test]
async fn quic_lookup_reports_wrong_role() -> Result<(), Error> {
    let server = server(NetworkType::Primary, Vec::new()).await?;
    let mut client = Client::new(2017, NetworkType::Worker(0), [server.address.clone()], TIMEOUT)?;
    assert!(matches!(client.lookup(signer(6).public()).await, Err(Error::NoCompatiblePeers)));
    Ok(())
}

/// A DHT serving a copy from another domain produces an authentication error, not a miss.
#[tokio::test]
async fn quic_lookup_reports_unverified_copies() -> Result<(), Error> {
    let key = signer(7);
    let invalid = signed_record(&key, 2018, NetworkType::Primary, 10, 1);
    let server = server(NetworkType::Primary, vec![invalid]).await?;
    let mut client = Client::new(2017, NetworkType::Primary, [server.address.clone()], TIMEOUT)?;
    assert!(matches!(client.lookup(key.public()).await, Err(Error::NoVerifiedRecords)));
    Ok(())
}
