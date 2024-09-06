// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Specific test utils for execution layer
use crate::{NetworkKeypair, WorkerId, WorkerInfo};
use fastcrypto::traits::KeyPair as _;

use rand::{rngs::StdRng, SeedableRng};

/// Fixture representing a worker for an [AuthorityFixture].
///
/// [WorkerFixture] holds keypairs and should not be used in production.
pub struct WorkerFixture {
    pub(crate) keypair: NetworkKeypair,
    #[allow(dead_code)]
    pub(crate) id: WorkerId,
    pub(crate) info: WorkerInfo,
}

impl WorkerFixture {
    pub fn keypair(&self) -> NetworkKeypair {
        self.keypair.copy()
    }

    pub fn info(&self) -> &WorkerInfo {
        &self.info
    }

    pub fn new_network(&self, router: anemo::Router) -> anemo::Network {
        anemo::Network::bind(self.info().worker_address.to_anemo_address().unwrap())
            .server_name("narwhal")
            .private_key(self.keypair().private().0.to_bytes())
            .start(router)
            .unwrap()
    }

    pub(crate) fn generate<R, P>(rng: R, id: WorkerId, mut get_port: P) -> Self
    where
        R: rand::RngCore + rand::CryptoRng,
        P: FnMut(&str) -> u16,
    {
        let keypair = NetworkKeypair::generate(&mut StdRng::from_rng(rng).unwrap());
        let worker_name = keypair.public().clone();
        let host = "127.0.0.1";
        let worker_address = format!("/ip4/{}/udp/{}", host, get_port(host)).parse().unwrap();
        let transactions = format!("/ip4/{}/tcp/{}/http", host, get_port(host)).parse().unwrap();

        Self { keypair, id, info: WorkerInfo { name: worker_name, worker_address, transactions } }
    }
}
