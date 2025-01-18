//! Helper methods for creating useful structs during tests.

use crate::execution::TransactionFactory;
use fastcrypto::{hash::Hash, traits::KeyPair as _};
use indexmap::IndexMap;
use rand::{
    distributions::Bernoulli,
    prelude::Distribution,
    rngs::{OsRng, StdRng},
    thread_rng, Rng, RngCore, SeedableRng,
};
use reth_primitives::{Address, BlockHash, Bytes, Header, TransactionSigned, U256};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    ops::RangeInclusive,
};
use tn_types::{
    adiri_chain_spec_arc, to_intent_message, AuthorityIdentifier, Batch, BlsKeypair, BlsSignature,
    Certificate, CertificateDigest, Committee, Epoch, HeaderBuilder, Multiaddr, NetworkKeypair,
    ProtocolSignature, Round, Stake, TimestampSec, WorkerId,
};

pub const VOTES_CF: &str = "votes";
pub const HEADERS_CF: &str = "headers";
pub const CERTIFICATES_CF: &str = "certificates";
pub const CERTIFICATE_DIGEST_BY_ROUND_CF: &str = "certificate_digest_by_round";
pub const CERTIFICATE_DIGEST_BY_ORIGIN_CF: &str = "certificate_digest_by_origin";
pub const PAYLOAD_CF: &str = "payload";

#[macro_export]
macro_rules! test_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
    };
}

/// Note: use the following macros to initialize your Primary / Consensus channels
/// if your test is spawning a primary and you encounter an `AllReg` error.
///
/// Rationale:
/// The primary initialization will try to edit a specific metric in its registry
/// for its new_certificates and committeed_certificates channel. The gauge situated
/// in the channel you're passing as an argument to the primary initialization is
/// the replacement. If that gauge is a dummy gauge, such as the one above, the
/// initialization of the primary will panic (to protect the production code against
/// an erroneous mistake in editing this bootstrap logic).
#[macro_export]
macro_rules! test_committed_certificates_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                primary::PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
                primary::PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
            )
            .unwrap(),
        );
    };
}

/// See (not imported to avoid a circular dependancy):
/// tn_primary_metrics::PrimaryChannelMetrics::NAME_NEW_CERTS,
pub const NAME_NEW_CERTS: &str = "tx_new_certificates";
/// See (not imported to avoid a circular dependancy):
/// tn_primary_metrics::PrimaryChannelMetrics::DESC_NEW_CERTS,
pub const DESC_NEW_CERTS: &str =
    "occupancy of the channel from the `Consensus` to the `primary::StateHandler`";

#[macro_export]
macro_rules! test_new_certificates_channel {
    ($e:expr) => {
        consensus_metrics::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                tn_types::test_utils::NAME_NEW_CERTS,
                tn_types::test_utils::DESC_NEW_CERTS,
            )
            .unwrap(),
        );
    };
}

pub fn temp_dir() -> std::path::PathBuf {
    tempfile::tempdir().expect("Failed to open temporary directory").into_path()
}

pub fn ensure_test_environment() {
    // One common issue when running tests on Mac is that the default ulimit is too low,
    // leading to I/O errors such as "Too many open files". Raising fdlimit to bypass it.
    // Also we can't do this in Windows, apparently.
    #[cfg(not(target_os = "windows"))]
    fdlimit::raise_fd_limit().expect("Could not raise ulimit");
}

pub fn test_network(keypair: NetworkKeypair, address: &Multiaddr) -> anemo::Network {
    let address = address.to_anemo_address().unwrap();
    let network_key = keypair.private().0.to_bytes();
    anemo::Network::bind(address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap()
}

pub fn random_network() -> anemo::Network {
    let network_key = NetworkKeypair::generate(&mut StdRng::from_rng(OsRng).unwrap());
    let address = "/ip4/127.0.0.1/udp/0".parse().unwrap();
    test_network(network_key, &address)
}

////////////////////////////////////////////////////////////////
// Keys, Committee
////////////////////////////////////////////////////////////////

pub fn random_key() -> BlsKeypair {
    BlsKeypair::generate(&mut thread_rng())
}

////////////////////////////////////////////////////////////////
// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////
pub fn fixture_payload(number_of_batches: u8) -> IndexMap<BlockHash, (WorkerId, TimestampSec)> {
    let mut payload: IndexMap<BlockHash, (WorkerId, TimestampSec)> = IndexMap::new();

    for _ in 0..number_of_batches {
        let batch_digest = batch().digest();

        payload.insert(batch_digest, (0, 0));
    }

    payload
}

/// will create a batch with randomly formed transactions
/// dictated by the parameter number_of_transactions
pub fn fixture_batch_with_transactions(number_of_transactions: u32) -> Batch {
    let transactions = (0..number_of_transactions).map(|_v| transaction()).collect();

    // Put some random bytes in the header so that tests will have unique headers.
    Batch { transactions, beneficiary: Address::random(), ..Default::default() }
}

pub fn fixture_payload_with_rand<R: Rng + ?Sized>(
    number_of_batches: u8,
    rand: &mut R,
) -> IndexMap<BlockHash, (WorkerId, TimestampSec)> {
    let mut payload: IndexMap<BlockHash, (WorkerId, TimestampSec)> = IndexMap::new();

    for _ in 0..number_of_batches {
        let batch_digest = batch_with_rand(rand).digest();

        payload.insert(batch_digest, (0, 0));
    }

    payload
}

/// Create a transaction with a randomly generated keypair.
pub fn transaction_with_rand<R: Rng + ?Sized>(rand: &mut R) -> TransactionSigned {
    let mut tx_factory = TransactionFactory::new_random_from_seed(rand);
    let chain = adiri_chain_spec_arc();
    // TODO: this is excessively high, but very unlikely to ever fail
    let gas_price = 875000000;
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // random transaction
    tx_factory.create_eip1559(chain, None, gas_price, Some(Address::ZERO), value, Bytes::new())
}

pub fn batch_with_rand<R: Rng + ?Sized>(rand: &mut R) -> Batch {
    Batch::new_for_test(
        vec![transaction_with_rand(rand), transaction_with_rand(rand)],
        Header::default(),
    )
}

// Fixture
pub fn transaction() -> TransactionSigned {
    // TODO: make this better
    //
    // The fn is complicated bc everything boils down to this fn
    // for seeding test data.
    //
    // gas price for adiri genesis: 875000000
    //
    // very inefficient, but less refactoring => quicker release

    // TODO: use [0; 32] seed account instead?
    let mut tx_factory = TransactionFactory::new_random();
    let chain = adiri_chain_spec_arc();
    let gas_price = 100_000;
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // random transaction
    tx_factory.create_eip1559(chain, None, gas_price, Some(Address::ZERO), value, Bytes::new())

    // // generate random value transactions, but the length will be always 100 bytes
    // (0..100).map(|_v| rand::random::<u8>()).collect()
}

////////////////////////////////////////////////////////////////
// Batches
////////////////////////////////////////////////////////////////

// Fixture
pub fn batch() -> Batch {
    let transactions = vec![transaction(), transaction()];
    Batch { transactions, ..Default::default() }
}

/// generate multiple fixture batches. The number of generated batches
/// are dictated by the parameter num_of_batches.
pub fn batches(num_of_batches: usize) -> Vec<Batch> {
    let mut batches = Vec::new();

    for i in 1..num_of_batches + 1 {
        batches.push(batch_with_transactions(i));
    }

    batches
}

pub fn batch_with_transactions(num_of_transactions: usize) -> Batch {
    let mut transactions = Vec::new();

    for _ in 0..num_of_transactions {
        transactions.push(transaction());
    }

    Batch::new_for_test(transactions, Header::default())
}

/// Creates one certificate per authority starting and finishing at the specified rounds
/// (inclusive).
///
/// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
/// of digests to be used as parents for the certificates of the next round.
///
/// Note : the certificates are unsigned
pub fn make_optimal_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_certificates(committee, range, initial_parents, ids, 0.0)
}

/// Outputs rounds worth of certificates with optimal parents that are signed.
pub fn make_optimal_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, BlsKeypair)],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_signed_certificates(range, initial_parents, committee, keys, 0.0)
}

/// Bernoulli-samples from a set of ancestors passed as a argument,
fn this_cert_parents(
    ancestors: &BTreeSet<CertificateDigest>,
    failure_prob: f64,
) -> BTreeSet<CertificateDigest> {
    std::iter::from_fn(|| {
        let f: f64 = rand::thread_rng().gen();
        Some(f > failure_prob)
    })
    .take(ancestors.len())
    .zip(ancestors)
    .flat_map(|(parenthood, parent)| parenthood.then_some(*parent))
    .collect::<BTreeSet<_>>()
}

/// Utility for making several rounds worth of certificates through iterated parenthood sampling.
/// The making of individual certificates once parents are figured out is delegated to the
/// `make_one_certificate` argument
fn rounds_of_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
    make_one_certificate: impl Fn(
        AuthorityIdentifier,
        Round,
        BTreeSet<CertificateDigest>,
    ) -> (CertificateDigest, Certificate),
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for id in ids {
            let this_cert_parents = this_cert_parents(&parents, failure_probability);

            let (digest, certificate) = make_one_certificate(*id, round, this_cert_parents);
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    ids: &[AuthorityIdentifier],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let generator = |pk, round, parents| mock_certificate(committee, pk, round, parents);

    rounds_of_certificates(range, initial_parents, ids, failure_probability, generator)
}

/// Creates certificates for the provided rounds but also having slow nodes.
///
/// `range`: the rounds for which we intend to create the certificates for
/// `initial_parents`: the parents to use when start creating the certificates
/// `keys`: the authorities for which it will create certificates for
/// `slow_nodes`: the authorities which are considered slow. Being a slow authority means that we
/// will  still create certificates for them on each round, but no other authority from higher round
/// will refer to those certificates. The number (by stake) of slow_nodes can not be > f , as
/// otherwise no valid graph will be produced.
pub fn make_certificates_with_slow_nodes(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: Vec<Certificate>,
    names: &[AuthorityIdentifier],
    slow_nodes: &[(AuthorityIdentifier, f64)],
) -> (VecDeque<Certificate>, Vec<Certificate>) {
    let mut rand = StdRng::seed_from_u64(1);

    // ensure provided slow nodes do not account > f
    let slow_nodes_stake: Stake =
        slow_nodes.iter().map(|(key, _)| committee.authority(key).unwrap().stake()).sum();

    assert!(slow_nodes_stake < committee.validity_threshold());

    let mut certificates = VecDeque::new();
    let mut parents = initial_parents;
    let mut next_parents = Vec::new();

    for round in range {
        next_parents.clear();
        for name in names {
            let this_cert_parents = this_cert_parents_with_slow_nodes(
                name,
                parents.clone(),
                slow_nodes,
                &mut rand,
                committee,
            );

            let (_, certificate) = mock_certificate(committee, *name, round, this_cert_parents);
            certificates.push_back(certificate.clone());
            next_parents.push(certificate);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

#[derive(Debug, Clone, Copy)]
pub enum TestLeaderSupport {
    /// There will be support for the leader, but less than f+1
    Weak,
    /// There will be strong support for the leader, meaning >= f+1
    Strong,
    /// Leader will be completely ommitted by the voters
    NoSupport,
}

pub struct TestLeaderConfiguration {
    /// The round of the leader
    pub round: Round,
    /// The leader id. That allow us to explicitly dictate which we consider the leader to be
    pub authority: AuthorityIdentifier,
    /// If true then the leader for that round will not be created at all
    pub should_omit: bool,
    /// The support that this leader should receive from the voters of next round
    pub support: Option<TestLeaderSupport>,
}

/// Creates fully connected DAG for the dictated rounds but with specific conditions for the
/// leaders.
///
/// By providing the `leader_configuration` we can dictate the setup for specific leaders
/// of specific rounds. For a leader the following can be configured:
/// * whether a leader will exist or not for a round
/// * whether a leader will receive enough support from the next round
pub fn make_certificates_with_leader_configuration(
    committee: &Committee,
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    names: &[AuthorityIdentifier],
    leader_configurations: HashMap<Round, TestLeaderConfiguration>,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    for round in leader_configurations.keys() {
        assert_eq!(round % 2, 0, "Leaders are elected only on even rounds");
    }

    let mut certificates: VecDeque<Certificate> = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();

        for name in names {
            // should we produce the leader of that round?
            if let Some(leader_config) = leader_configurations.get(&round) {
                if leader_config.should_omit && leader_config.authority == *name {
                    // just skip and don't create the certificate for this authority
                    continue;
                }
            }

            // we now check for the leader of previous round. If should not be omitted we need to
            // check on the support we are supposed to provide
            let cert_parents = if round > 0 {
                if let Some(leader_config) = leader_configurations.get(&(round - 1)) {
                    match leader_config.support {
                        Some(TestLeaderSupport::Weak) => {
                            // find the leader from the previous round
                            let leader_certificate = certificates
                                .iter()
                                .find(|c| {
                                    c.round() == round - 1 && c.origin() == leader_config.authority
                                })
                                .unwrap();

                            // check whether anyone from the current round already included it
                            // if yes, then we should remove it and not vote again.
                            if certificates.iter().any(|c| {
                                c.round() == round
                                    && c.header().parents().contains(&leader_certificate.digest())
                            }) {
                                let mut p = parents.clone();
                                p.remove(&leader_certificate.digest());
                                p
                            } else {
                                // otherwise return all the parents
                                parents.clone()
                            }
                        }
                        Some(TestLeaderSupport::Strong) => {
                            // just return the whole parent set so we can vote for it
                            parents.clone()
                        }
                        Some(TestLeaderSupport::NoSupport) => {
                            // remove the leader from the set of parents
                            let c = certificates
                                .iter()
                                .find(|c| {
                                    c.round() == round - 1 && c.origin() == leader_config.authority
                                })
                                .unwrap();
                            let mut p = parents.clone();
                            p.remove(&c.digest());
                            p
                        }
                        None => parents.clone(),
                    }
                } else {
                    parents.clone()
                }
            } else {
                parents.clone()
            };

            // Create the certificates
            let (_, certificate) = mock_certificate(committee, *name, round, cert_parents);
            certificates.push_back(certificate.clone());
            next_parents.insert(certificate.digest());
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// Returns the parents that should be used as part of a newly created certificate.
///
/// The `slow_nodes` parameter is used to dictate which parents to exclude and not use. The slow
/// node will not be used under some probability which is provided as part of the tuple.
/// If probability to use it is 0.0, then the parent node will NEVER be used.
/// If probability to use it is 1.0, then the parent node will ALWAYS be used.
/// We always make sure to include our "own" certificate, thus the `name` property is needed.
pub fn this_cert_parents_with_slow_nodes(
    authority_id: &AuthorityIdentifier,
    ancestors: Vec<Certificate>,
    slow_nodes: &[(AuthorityIdentifier, f64)],
    rand: &mut StdRng,
    committee: &Committee,
) -> BTreeSet<CertificateDigest> {
    let mut parents = BTreeSet::new();
    let mut not_included = Vec::new();
    let mut total_stake = 0;

    for parent in ancestors {
        let authority = committee.authority(&parent.origin()).unwrap();

        // Identify if the parent is within the slow nodes - and is not the same author as the
        // one we want to create the certificate for.
        if let Some((_, inclusion_probability)) = slow_nodes
            .iter()
            .find(|(id, _)| *id != *authority_id && *id == parent.header().author())
        {
            let b = Bernoulli::new(*inclusion_probability).unwrap();
            let should_include = b.sample(rand);

            if should_include {
                parents.insert(parent.digest());
                total_stake += authority.stake();
            } else {
                not_included.push(parent);
            }
        } else {
            // just add it directly as it is not within the slow nodes or we are the
            // same author.
            parents.insert(parent.digest());
            total_stake += authority.stake();
        }
    }

    // ensure we'll have enough parents (2f + 1)
    while total_stake < committee.quorum_threshold() {
        let parent = not_included.pop().unwrap();
        let authority = committee.authority(&parent.origin()).unwrap();

        total_stake += authority.stake();

        parents.insert(parent.digest());
    }

    assert!(
        committee.reached_quorum(total_stake),
        "Not enough parents by stake provided. Expected at least {} but instead got {}",
        committee.quorum_threshold(),
        total_stake
    );

    parents
}

/// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates_with_epoch(
    committee: &Committee,
    range: RangeInclusive<Round>,
    epoch: Epoch,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[AuthorityIdentifier],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) =
                mock_certificate_with_epoch(committee, *name, round, epoch, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents.clone_from(&next_parents);
    }
    (certificates, next_parents)
}

/// make rounds worth of signed certificates with the sampled number of parents
pub fn make_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    committee: &Committee,
    keys: &[(AuthorityIdentifier, BlsKeypair)],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    let ids = keys.iter().map(|(authority, _)| *authority).collect::<Vec<_>>();
    let generator =
        |pk, round, parents| mock_signed_certificate(keys, pk, round, parents, committee);

    rounds_of_certificates(range, initial_parents, &ids[..], failure_probability, generator)
}

pub fn mock_certificate_with_rand<R: RngCore + ?Sized>(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    rand: &mut R,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(0)
        .parents(parents)
        .payload(fixture_payload_with_rand(1, rand))
        .build()
        .unwrap();
    let certificate = Certificate::new_unsigned(committee, header, Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates a badly signed certificate from its given round, origin and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    mock_certificate_with_epoch(committee, origin, round, 0, parents)
}

/// Creates a badly signed certificate from its given round, epoch, origin, and parents,
/// Note: the certificate is signed by a random key rather than its author
pub fn mock_certificate_with_epoch(
    committee: &Committee,
    origin: AuthorityIdentifier,
    round: Round,
    epoch: Epoch,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(origin)
        .round(round)
        .epoch(epoch)
        .parents(parents)
        .payload(fixture_payload(1))
        .build()
        .unwrap();
    let certificate = Certificate::new_unsigned(committee, header, Vec::new()).unwrap();
    (certificate.digest(), certificate)
}

/// Creates one signed certificate from a set of signers - the signers must include the origin
pub fn mock_signed_certificate(
    signers: &[(AuthorityIdentifier, BlsKeypair)],
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    committee: &Committee,
) -> (CertificateDigest, Certificate) {
    let header_builder = HeaderBuilder::default()
        .author(origin)
        .payload(fixture_payload(1))
        .round(round)
        .epoch(0)
        .parents(parents);

    let header = header_builder.build().unwrap();

    let cert = Certificate::new_unsigned(committee, header.clone(), Vec::new()).unwrap();

    let mut votes = Vec::new();
    for (name, signer) in signers {
        let sig = BlsSignature::new_secure(&to_intent_message(cert.header().digest()), signer);
        votes.push((*name, sig))
    }
    let cert = Certificate::new_unverified(committee, header, votes).unwrap();
    (cert.digest(), cert)
}
