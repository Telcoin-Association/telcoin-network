//! Authority fixture for the cluster

use crate::WorkerFixture;
use std::num::NonZeroUsize;
use tn_config::{Config, ConsensusConfig, KeyConfig, NetworkConfig};
use tn_types::{
    Address, Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey, Certificate, Committee,
    Database, Hash as _, Header, HeaderBuilder, Multiaddr, NetworkKeypair, NetworkPublicKey, Round,
    Vote, WorkerCache,
};

/// Fixture representing an validator node within the network.
///
/// [AuthorityFixture] holds keypairs and should not be used in production.
#[derive(Debug)]
pub struct AuthorityFixture<DB> {
    /// Thread-safe cell with a reference to the [Authority] struct used in production.
    authority: Authority,
    /// All workers for this authority as a [WorkerFixture].
    worker: WorkerFixture,
    /// Config for this authority.
    consensus_config: ConsensusConfig<DB>,
    /// The testing primary key.
    primary_keypair: BlsKeypair,
}

impl<DB: Database> AuthorityFixture<DB> {
    /// The owned [AuthorityIdentifier] for the authority
    pub fn id(&self) -> AuthorityIdentifier {
        self.authority.id()
    }

    /// The [Authority] struct used in production.
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// The authority's bls12381 [KeyPair] used to sign consensus messages.
    pub fn keypair(&self) -> &BlsKeypair {
        &self.primary_keypair
    }

    /// The authority's ed25519 [NetworkKeypair] used to sign messages on the network.
    pub fn primary_network_keypair(&self) -> &NetworkKeypair {
        self.consensus_config.key_config().primary_network_keypair()
    }

    /// The authority's [Address] for execution layer.
    pub fn execution_address(&self) -> Address {
        self.authority.execution_address()
    }

    /// A reference to the authority's [Multiaddr] on the consensus network.
    pub fn network_address(&self) -> &Multiaddr {
        self.authority.primary_network_address()
    }

    /// Return a reference to a [WorkerFixture] for this authority.
    pub fn worker(&self) -> &WorkerFixture {
        &self.worker
    }

    /// The authority's [PublicKey].
    pub fn primary_public_key(&self) -> BlsPublicKey {
        self.consensus_config.key_config().primary_public_key()
    }

    /// The authority's [NetworkPublicKey].
    pub fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.consensus_config.key_config().primary_network_public_key()
    }

    /// Create a [Header] with a default payload based on the [Committee] argument.
    pub fn header(&self, committee: &Committee) -> Header {
        self.header_builder(committee).build()
    }

    /// Create a [Header] with a default payload based on the [Committee] and [Round] arguments.
    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        self.header_builder(committee).payload(Default::default()).round(round).build()
    }

    /// Return a [HeaderV1Builder] for round 1. The builder is constructed
    /// with a genesis certificate as the parent.
    pub fn header_builder(&self, committee: &Committee) -> HeaderBuilder {
        HeaderBuilder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
    }

    /// Sign a [Header] and return a [Vote] with no additional validation.
    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new_sync(header, self.id(), self.consensus_config.key_config())
    }

    /// Return the consensus config.
    pub fn consensus_config(&self) -> ConsensusConfig<DB> {
        self.consensus_config.clone()
    }

    /// Generate a new [AuthorityFixture].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn generate(
        number_of_workers: NonZeroUsize,
        authority: Authority,
        keys: (BlsKeypair, KeyConfig),
        committee: Committee,
        db: DB,
        worker: WorkerFixture,
        worker_cache: WorkerCache,
        network_config: NetworkConfig,
    ) -> Self {
        let (primary_keypair, key_config) = keys;
        // Make sure our keys are correct.
        assert_eq!(&key_config.primary_public_key(), authority.protocol_key());
        assert_eq!(key_config.primary_network_public_key(), authority.network_key());
        assert_eq!(primary_keypair.public(), &key_config.primary_public_key());
        // Currently only support one worker per node.
        // If/when this is relaxed then the key_config below will need to change.
        assert_eq!(number_of_workers.get(), 1);
        let mut config = Config::default_for_test();
        // These key updates don't return errors...
        let _ = config.update_protocol_key(key_config.primary_public_key());
        let _ = config.update_primary_network_key(key_config.primary_network_public_key());
        let _ = config.update_worker_network_key(key_config.worker_network_public_key());
        config.node_info.p2p_info.network_address = authority.primary_network_address().clone();

        let consensus_config = ConsensusConfig::new_with_committee_for_test(
            config,
            db,
            key_config.clone(),
            committee,
            worker_cache,
            network_config,
        )
        .expect("failed to generate config!");

        Self { authority, worker, consensus_config, primary_keypair }
    }
}
