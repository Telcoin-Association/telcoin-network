//! Authority fixture for the cluster

use crate::WorkerFixture;
use std::num::NonZeroUsize;
use tn_config::{Config, ConsensusConfig, KeyConfig, NetworkConfig, Parameters};
use tn_types::{
    Address, Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey, BlsSignature, Certificate,
    Committee, Database, Epoch, EpochDigest, EpochSeedMessage, Genesis, Hash as _, Header,
    HeaderBuilder, NetworkKeypair, NetworkPublicKey, Round, Vote,
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
    ///
    /// The seed signature is re-stamped for `round`: the seed message binds the header's round, so
    /// carrying the round-1 signature from [`Self::header_builder`] would produce a header every
    /// honest voter refuses.
    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        self.header_builder(committee)
            .payload(Default::default())
            .round(round)
            .seed_signature(self.seed_signature(committee.epoch(), round))
            .build()
    }

    /// Return a [HeaderV1Builder] for round 1. The builder is constructed
    /// with a genesis certificate as the parent.
    ///
    /// The builder is seeded with this authority's valid seed signature for round 1 (anchored to
    /// this authority's configured prior-epoch digest), so fixture-built headers pass the vote
    /// path's seed-signature verification against a config carrying the same anchor. Any caller
    /// that changes the builder's round MUST also re-stamp the signature for the new round with
    /// [`Self::seed_signature`], because the seed message binds the round.
    pub fn header_builder(&self, committee: &Committee) -> HeaderBuilder {
        HeaderBuilder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
            .seed_signature(self.seed_signature(committee.epoch(), 1))
    }

    /// This authority's deterministic BLS signature over the canonical seed message for
    /// `(epoch, round)`, anchored to this authority's configured prior-epoch digest (see
    /// [`ConsensusConfig::prior_epoch_record`]). The anchor defaults to [`EpochDigest::default`]
    /// unless the [`CommitteeFixture`](crate::CommitteeFixture) builder was given a non-default
    /// one, so a fixture header verifies against a voter configured with the same anchor.
    pub fn seed_signature(&self, epoch: Epoch, round: Round) -> BlsSignature {
        EpochSeedMessage::new(epoch, round, self.consensus_config.prior_epoch_record())
            .sign(self.consensus_config.key_config())
    }

    /// Sign a [Header] and return a [Vote] with no additional validation.
    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new(header, self.id(), self.consensus_config.key_config())
    }

    /// Return the consensus config.
    pub fn consensus_config(&self) -> ConsensusConfig<DB> {
        self.consensus_config.clone()
    }

    /// Generate a new [AuthorityFixture].
    ///
    /// `prior_epoch_record` is the cross-epoch anchor the authority's [`ConsensusConfig`] carries
    /// (see [`ConsensusConfig::prior_epoch_record`]); it is [`EpochDigest::default`] for the usual
    /// epoch-0 fixture and non-default only when the builder was given an explicit anchor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn generate(
        number_of_workers: NonZeroUsize,
        authority: Authority,
        keys: (BlsKeypair, KeyConfig),
        committee: Committee,
        db: DB,
        worker: WorkerFixture,
        network_config: NetworkConfig,
        genesis: Genesis,
        parameters: &Option<Parameters>,
        prior_epoch_record: EpochDigest,
    ) -> Self {
        let (primary_keypair, key_config) = keys;
        // Make sure our keys are correct.
        assert_eq!(&key_config.primary_public_key(), authority.protocol_key());
        assert_eq!(primary_keypair.public(), &key_config.primary_public_key());
        // Currently only support one worker per node.
        // If/when this is relaxed then the key_config below will need to change.
        assert_eq!(number_of_workers.get(), 1);
        let mut config = Config::default_for_test_with_genesis(genesis);
        // overwrite default parameters if provided
        if let Some(overwrite) = parameters {
            config.parameters = overwrite.clone();
        }
        // These key updates don't return errors...
        let _ = config.update_protocol_key(key_config.primary_public_key());
        let _ = config.update_primary_network_key(key_config.primary_network_public_key());
        let _ = config.update_worker_network_key(key_config.worker_network_public_key());

        let consensus_config = ConsensusConfig::new_with_committee_and_prior_epoch_record_for_test(
            config,
            db,
            key_config.clone(),
            committee,
            network_config,
            prior_epoch_record,
        )
        .expect("failed to generate config!");

        Self { authority, worker, consensus_config, primary_keypair }
    }
}
