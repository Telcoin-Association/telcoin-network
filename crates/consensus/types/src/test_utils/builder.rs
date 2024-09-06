// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! The builder responsible for creating all aspects of the committee fixture.
use super::{AuthorityFixture, CommitteeFixture};
use crate::{utils::get_available_tcp_port, CommitteeBuilder, Epoch, Stake};
use fastcrypto::traits::KeyPair as _;
use once_cell::sync::OnceCell;
use rand::{
    rngs::{OsRng, StdRng},
    SeedableRng,
};
use std::{collections::VecDeque, num::NonZeroUsize};

pub struct Builder<R = OsRng> {
    rng: R,
    committee_size: NonZeroUsize,
    number_of_workers: NonZeroUsize,
    randomize_ports: bool,
    epoch: Epoch,
    stake: VecDeque<Stake>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Self {
            epoch: Epoch::default(),
            rng: OsRng,
            committee_size: NonZeroUsize::new(4).unwrap(),
            number_of_workers: NonZeroUsize::new(4).unwrap(),
            randomize_ports: false,
            stake: VecDeque::new(),
        }
    }
}

impl<R> Builder<R> {
    pub fn committee_size(mut self, committee_size: NonZeroUsize) -> Self {
        self.committee_size = committee_size;
        self
    }

    pub fn number_of_workers(mut self, number_of_workers: NonZeroUsize) -> Self {
        self.number_of_workers = number_of_workers;
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

    pub fn stake_distribution(mut self, stake: VecDeque<Stake>) -> Self {
        self.stake = stake;
        self
    }

    pub fn rng<N: rand::RngCore + rand::CryptoRng>(self, rng: N) -> Builder<N> {
        Builder {
            rng,
            epoch: self.epoch,
            committee_size: self.committee_size,
            number_of_workers: self.number_of_workers,
            randomize_ports: self.randomize_ports,
            stake: self.stake,
        }
    }
}

impl<R: rand::RngCore + rand::CryptoRng> Builder<R> {
    pub fn build(mut self) -> CommitteeFixture {
        if !self.stake.is_empty() {
            assert_eq!(self.stake.len(), self.committee_size.get(), "Stake vector has been provided but is different length the committee - it should be the same");
        }

        let mut authorities: Vec<AuthorityFixture> = (0..self.committee_size.get())
            .map(|_| {
                AuthorityFixture::generate(
                    StdRng::from_rng(&mut self.rng).unwrap(),
                    self.number_of_workers,
                    |host| {
                        if self.randomize_ports {
                            get_available_tcp_port(host).unwrap_or_default()
                        } else {
                            0
                        }
                    },
                )
            })
            .collect();

        // now order the AuthorityFixtures by the authority BlsPublicKey so when we iterate either
        // via the committee.authorities() or via the fixture.authorities() we'll get the
        // same order.
        authorities.sort_by_key(|a1| a1.public_key());

        // create the committee in order to assign the ids to the authorities
        let mut committee_builder = CommitteeBuilder::new(self.epoch);
        for a in authorities.iter() {
            committee_builder.add_authority(
                a.public_key().clone(),
                self.stake.pop_front().unwrap_or(1),
                a.network_address.clone(),
                a.execution_address,
                a.network_public_key(),
                a.network_address.to_string(),
            );
        }
        let committee = committee_builder.build();

        // Update the Fixtures with the id assigned from the committee
        for authority in authorities.iter_mut() {
            let a = committee.authority_by_key(authority.keypair.public()).unwrap();
            authority.authority = OnceCell::with_value(a.clone());
            authority.stake = a.stake();
        }

        // Now update the stake to follow the order of the authorities so we produce expected
        // results
        let authorities: Vec<AuthorityFixture> = authorities
            .into_iter()
            .map(|mut authority| {
                authority.stake = self.stake.pop_front().unwrap_or(1);
                authority
            })
            .collect();

        CommitteeFixture { authorities, committee, epoch: self.epoch }
    }
}
