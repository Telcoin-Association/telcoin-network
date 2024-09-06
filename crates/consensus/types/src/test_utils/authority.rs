// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority fixture for holding all keypairs and workers for a validator node within a committee.
use super::WorkerFixture;
use crate::{
    Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey, Certificate, Committee, Header,
    HeaderV1Builder, Multiaddr, NetworkKeypair, NetworkPublicKey, Round, Stake, Vote, WorkerId,
    WorkerIndex,
};
use fastcrypto::{
    hash::Hash,
    traits::{AllowedRng, KeyPair as _},
};
use once_cell::sync::OnceCell;
use reth_primitives::Address;
use std::{collections::BTreeMap, num::NonZeroUsize};

/// Fixture representing an validator node within the network.
///
/// [AuthorityFixture] holds keypairs and should not be used in production.
pub struct AuthorityFixture {
    /// Thread-safe cell with a reference to the [Authority] struct used in production.
    pub(crate) authority: OnceCell<Authority>,
    /// The [KeyPair] for this authority.
    pub(crate) keypair: BlsKeypair,
    /// The [NetworkKeypair] for this authority.
    pub(crate) network_keypair: NetworkKeypair,
    /// The [Stake] for this authority.
    pub(crate) stake: Stake,
    /// The [Multiaddr] within the anemo network for this authority.
    pub(crate) network_address: Multiaddr,
    /// All workers for this authority mapped by [WorkerId] to [WorkerFixture].
    pub(crate) workers: BTreeMap<WorkerId, WorkerFixture>,
    /// The address for the authority on the EL.
    pub(crate) execution_address: Address,
}

impl AuthorityFixture {
    /// The owned [AuthorityIdentifier] for the authority
    pub fn id(&self) -> AuthorityIdentifier {
        self.authority.get().unwrap().id()
    }

    /// The [Authority] struct used in production.
    pub fn authority(&self) -> &Authority {
        self.authority.get().unwrap()
    }

    /// The authority's bls12381 [KeyPair] used to sign consensus messages.
    pub fn keypair(&self) -> &BlsKeypair {
        &self.keypair
    }

    /// The authority's ed25519 [NetworkKeypair] used to sign messages on the network.
    pub fn network_keypair(&self) -> NetworkKeypair {
        self.network_keypair.copy()
    }

    /// The authority's [Address] for execution layer.
    pub fn execution_address(&self) -> Address {
        self.execution_address
    }

    /// Create a new anemo network for consensus.
    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.network_address.to_anemo_address().unwrap())
            .server_name("narwhal")
            .private_key(self.network_keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    /// A reference to the authority's [Multiaddr] on the consensus network.
    pub fn network_address(&self) -> &Multiaddr {
        &self.network_address
    }

    /// Return a reference to a [WorkerFixture] based on the provided [WorkerId].
    pub fn worker(&self, id: WorkerId) -> &WorkerFixture {
        self.workers.get(&id).unwrap()
    }

    /// A `Vec<NetworkKeypair>` of all workers for the authority.
    pub fn worker_keypairs(&self) -> Vec<NetworkKeypair> {
        self.workers.values().map(|worker| worker.keypair.copy()).collect()
    }

    /// The authority's [PublicKey].
    pub fn public_key(&self) -> BlsPublicKey {
        self.keypair.public().clone()
    }

    /// The authority's [NetworkPublicKey].
    pub fn network_public_key(&self) -> NetworkPublicKey {
        self.network_keypair.public().clone()
    }

    /// The [WorkerIndex] for the authority.
    pub fn worker_index(&self) -> WorkerIndex {
        WorkerIndex(self.workers.iter().map(|(id, w)| (*id, w.info.clone())).collect())
    }

    /// Create a [Header] with a default payload based on the [Committee] argument.
    pub fn header(&self, committee: &Committee) -> Header {
        let header = self.header_builder(committee).payload(Default::default()).build().unwrap();
        Header::V1(header)
    }

    /// Create a [Header] with a default payload based on the [Committee] and [Round] arguments.
    pub fn header_with_round(&self, committee: &Committee, round: Round) -> Header {
        let header = self
            .header_builder(committee)
            .payload(Default::default())
            .round(round)
            .build()
            .unwrap();
        Header::V1(header)
    }

    /// Return a [HeaderV1Builder] for round 1. The builder is constructed
    /// with a genesis certificate as the parent.
    pub fn header_builder(&self, committee: &Committee) -> HeaderV1Builder {
        HeaderV1Builder::default()
            .author(self.id())
            .round(1)
            .epoch(committee.epoch())
            .parents(Certificate::genesis(committee).iter().map(|x| x.digest()).collect())
    }

    /// Sign a [Header] and return a [Vote] with no additional validation.
    pub fn vote(&self, header: &Header) -> Vote {
        Vote::new_with_signer(header, &self.id(), &self.keypair)
    }

    /// Generate a new [AuthorityFixture].
    pub(crate) fn generate<R, P>(
        mut rng: R,
        number_of_workers: NonZeroUsize,
        mut get_port: P,
    ) -> Self
    where
        R: AllowedRng,
        P: FnMut(&str) -> u16,
    {
        let keypair = BlsKeypair::generate(&mut rng);
        let network_keypair = NetworkKeypair::generate(&mut rng);
        let host = "127.0.0.1";
        let network_address: Multiaddr =
            format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();
        let execution_address = Address::random_with(&mut rng);

        let workers = (0..number_of_workers.get())
            .map(|idx| {
                let worker = WorkerFixture::generate(&mut rng, idx as u16, &mut get_port);

                (idx as u16, worker)
            })
            .collect();

        Self {
            authority: OnceCell::new(),
            keypair,
            network_keypair,
            stake: 1,
            network_address,
            workers,
            execution_address,
        }
    }
}
