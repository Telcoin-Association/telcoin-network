// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Committe fixture for all authorities and their workers within a committee for a specific epoch.
use super::{fixture_batch_with_transactions, AuthorityFixture, Builder};
use crate::{
    Certificate, CertificateDigest, Committee, Epoch, Header, HeaderAPI, HeaderV1Builder, Round,
    Vote, VoteAPI, WorkerCache,
};
use std::collections::BTreeSet;

/// Fixture representing a committee to reach consensus.
///
/// The [CommitteeFixture] holds all authorities.
pub struct CommitteeFixture {
    /// The collection of [AuthorityFixture]s that comprise the committee.
    pub(crate) authorities: Vec<AuthorityFixture>,
    /// The [Committee] used in production.
    pub(crate) committee: Committee,
    /// The [Epoch] the the [Committee] is responsible for reaching consensus.
    pub(crate) epoch: Epoch,
}

impl CommitteeFixture {
    /// Return an Iterator for [AuthorityFixture] references.
    pub fn authorities(&self) -> impl Iterator<Item = &AuthorityFixture> {
        self.authorities.iter()
    }

    /// Return a builder for the [CommitteeFixture].
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Return the [Committee] for the fixture.
    pub fn committee(&self) -> Committee {
        self.committee.clone()
    }

    /// Return the [WorkerCache] for the committee.
    pub fn worker_cache(&self) -> WorkerCache {
        WorkerCache {
            epoch: self.epoch,
            workers: self.authorities.iter().map(|a| (a.public_key(), a.worker_index())).collect(),
        }
    }

    // pub fn header(&self, author: BlsPublicKey) -> Header {
    // Currently sign with the last authority

    /// Return a header from the last authority in the committee.
    ///
    /// See [AuthorityFixture::header()] for more information.
    pub fn header(&self) -> Header {
        self.authorities.last().unwrap().header(&self.committee())
    }

    /// Return a `Vec<Header>` - one [Header] per authority in the committee.
    ///
    /// See [AuthorityFixture::header_with_round()] for more information.
    /// Currently only builds a header for hard-coded round `1`.
    pub fn headers(&self) -> Vec<Header> {
        let committee = self.committee();

        self.authorities.iter().map(|a| a.header_with_round(&committee, 1)).collect()
    }

    /// Return a `Vec<Header>` - one [Header] per authority in the committee for round 2.
    ///
    /// See [AuthorityFixture::header_with_round()] for more information.
    /// Currently only builds a header for hard-coded round `2`.
    pub fn headers_next_round(&self) -> Vec<Header> {
        let committee = self.committee();
        self.authorities.iter().map(|a| a.header_with_round(&committee, 2)).collect()
    }

    /// Return a `Vec<Header>` for the next round - one [Header] per authority in the committee.
    ///
    /// Uses the [HeaderV1Builder] to construct a collection of headers for the next round.
    pub fn headers_round(
        &self,
        prior_round: Round,
        parents: &BTreeSet<CertificateDigest>,
    ) -> (Round, Vec<Header>) {
        let round = prior_round + 1;
        let next_headers = self
            .authorities
            .iter()
            .map(|a| {
                let builder = HeaderV1Builder::default();
                let header = builder
                    .author(a.id())
                    .round(round)
                    .epoch(0)
                    .parents(parents.clone())
                    .with_payload_batch(fixture_batch_with_transactions(10), 0, 0)
                    .build()
                    .unwrap();
                Header::V1(header)
            })
            .collect();

        (round, next_headers)
    }

    /// Collect [Vote]s for a header based on the current committee.
    ///
    /// Note: the authority for the voted-on header is skipped.
    pub fn votes(&self, header: &Header) -> Vec<Vote> {
        self.authorities()
            .flat_map(|a| {
                // we should not re-sign using the key of the authority
                // that produced the header
                if a.id() == header.author() {
                    None
                } else {
                    Some(a.vote(header))
                }
            })
            .collect()
    }

    /// Create a [Certificate] for a header by casting votes from all authorities in the committee.
    ///
    /// See also [`Certificate::new_unverified`] and [`Self::votes`].
    pub fn certificate(&self, header: &Header) -> Certificate {
        let committee = self.committee();
        let votes: Vec<_> =
            self.votes(header).into_iter().map(|x| (x.author(), x.signature().clone())).collect();
        Certificate::new_unverified(&committee, header.clone(), votes).unwrap()
    }
}
