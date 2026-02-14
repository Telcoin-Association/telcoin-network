//! The builder responsible for creating all aspects of the committee fixture.

use crate::WorkerFixture;

use super::{AuthorityFixture, CommitteeFixture};
use rand::{rngs::StdRng, SeedableRng};
use std::{collections::BTreeMap, marker::PhantomData, num::NonZeroUsize};
use tn_config::{KeyConfig, NetworkConfig, Parameters};
use tn_types::{
    get_available_udp_port, test_genesis, Address, Authority, AuthorityIdentifier, BlsKeypair,
    BootstrapServer, Committee, Database, Epoch, Multiaddr, TimestampSec, DEFAULT_WORKER_PORT,
};

/// The committee builder for tests.
#[derive(Debug)]
pub struct Builder<DB, F, R = StdRng> {
    rng: R,
    committee_size: NonZeroUsize,
    number_of_workers: NonZeroUsize,
    randomize_ports: bool,
    epoch: Epoch,
    network_config: Option<NetworkConfig>,
    epoch_boundary: Option<TimestampSec>,
    new_db: F,
    consensus_parameters: Option<Parameters>,
    _phantom_data: PhantomData<DB>,
}

impl<DB, F> Builder<DB, F>
where
    DB: Database,
    F: Fn() -> DB,
{
    pub fn new(new_db: F) -> Self {
        Self {
            rng: StdRng::from_os_rng(),
            epoch: Epoch::default(),
            committee_size: NonZeroUsize::new(4).unwrap(),
            number_of_workers: NonZeroUsize::new(1).unwrap(),
            randomize_ports: false,
            network_config: None,
            epoch_boundary: None,
            new_db,
            consensus_parameters: None,
            _phantom_data: PhantomData::<DB>,
        }
    }
}

impl<DB, F, R> Builder<DB, F, R>
where
    DB: Database,
    F: Fn() -> DB,
{
    pub fn committee_size(mut self, committee_size: NonZeroUsize) -> Self {
        self.committee_size = committee_size;
        self
    }

    pub fn randomize_ports(mut self, randomize_ports: bool) -> Self {
        self.randomize_ports = randomize_ports;
        self
    }

    pub fn epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn with_network_config(mut self, network_config: NetworkConfig) -> Self {
        self.network_config = Some(network_config);
        self
    }

    /// Include a timestamp (in secs) for closing the epoch.
    pub fn with_epoch_boundary(mut self, epoch_boundary: TimestampSec) -> Self {
        self.epoch_boundary = Some(epoch_boundary);
        self
    }

    /// Overwrite the default consensus parameters.
    pub fn with_consensus_parameters(mut self, parameters: Parameters) -> Self {
        self.consensus_parameters = Some(parameters);
        self
    }

    /// Use a provided rng. This is useful for deterministic testing.
    pub fn with_rng<RNG: rand::RngCore + rand::CryptoRng>(self, rng: RNG) -> Builder<DB, F, RNG> {
        Builder {
            rng,
            epoch: self.epoch,
            committee_size: self.committee_size,
            number_of_workers: self.number_of_workers,
            randomize_ports: self.randomize_ports,
            network_config: None,
            epoch_boundary: None,
            new_db: self.new_db,
            consensus_parameters: self.consensus_parameters,
            _phantom_data: PhantomData::<DB>,
        }
    }
}

impl<DB, F> Builder<DB, F>
where
    DB: Database,
    F: Fn() -> DB,
{
    pub fn build(mut self) -> CommitteeFixture<DB> {
        let committee_size = self.committee_size.get();
        let network_config = self.network_config.unwrap_or_default();

        let mut rng = StdRng::from_rng(&mut self.rng);
        let mut committee_info = Vec::with_capacity(committee_size);
        #[allow(clippy::mutable_key_type)]
        let mut authorities = BTreeMap::new();
        let mut bootstrap_servers = BTreeMap::new();
        // Pass 1 to make the authorities so we can make the committee struct we need later.
        for _ in 0..committee_size {
            let primary_keypair = BlsKeypair::generate(&mut rng);
            let key_config = KeyConfig::new_with_testing_key(primary_keypair.copy());
            let host = "127.0.0.1";
            let port = if self.randomize_ports {
                get_available_udp_port(host).unwrap_or(DEFAULT_WORKER_PORT)
            } else {
                0
            };
            let primary_network_address: Multiaddr =
                format!("/ip4/{host}/udp/{port}/quic-v1").parse().unwrap();
            let port = if self.randomize_ports {
                get_available_udp_port(host).unwrap_or(DEFAULT_WORKER_PORT)
            } else {
                0
            };
            let worker_network_address: Multiaddr =
                format!("/ip4/{host}/udp/{port}/quic-v1").parse().unwrap();
            let authority = Authority::new_for_test(
                key_config.primary_public_key(),
                1,
                Address::random_with(&mut rng),
            );
            bootstrap_servers.insert(
                *authority.protocol_key(),
                BootstrapServer::new(
                    (primary_network_address, key_config.primary_network_public_key()).into(),
                    (worker_network_address, key_config.worker_network_public_key()).into(),
                ),
            );
            authorities.insert(
                *authority.protocol_key(),
                (primary_keypair, key_config, authority.clone()),
            );
        }
        // Reset the authority ids so they are in sort order.  Some tests require this.
        for (i, (_, (primary_keypair, key_config, authority))) in authorities.iter_mut().enumerate()
        {
            let worker = WorkerFixture::generate(key_config.clone(), i as u16);
            committee_info.push((
                primary_keypair.copy(),
                key_config.clone(),
                authority.clone(),
                worker,
                network_config.clone(),
            ));
        }
        // Make the committee so we can give it the AuthorityFixtures below.
        let committee = Committee::new_for_test(
            authorities.into_iter().map(|(k, (_, _, a))| (k, a)).collect(),
            0,
            bootstrap_servers,
        );
        let genesis = test_genesis();
        // All the authorities use the same worker cache.
        let authorities: BTreeMap<AuthorityIdentifier, AuthorityFixture<DB>> = committee_info
            .into_iter()
            .map(|(primary_keypair, key_config, authority, worker, network_config)| {
                (
                    authority.id(),
                    AuthorityFixture::generate(
                        self.number_of_workers,
                        authority,
                        (primary_keypair, key_config),
                        committee.clone(),
                        (self.new_db)(),
                        worker,
                        network_config,
                        genesis.clone(),
                        &self.consensus_parameters,
                    ),
                )
            })
            .collect();

        CommitteeFixture { authorities, committee }
    }
}
