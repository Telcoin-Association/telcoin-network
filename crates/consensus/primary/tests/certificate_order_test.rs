// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use fastcrypto::traits::KeyPair as _;
use indexmap::IndexMap;
use rand::{rngs::OsRng, seq::SliceRandom};
use std::{collections::BTreeSet, num::NonZeroUsize};
use tn_types::{
    test_utils::{AuthorityFixture, CommitteeFixture},
    AuthorityIdentifier, BlsPublicKey, BlsSignature, Certificate, Committee, Header, Stake, Vote,
};

#[tokio::test]
async fn test_certificate_signers_are_ordered() {
    // GIVEN
    let fixture = CommitteeFixture::builder()
        .committee_size(NonZeroUsize::new(4).unwrap())
        .stake_distribution((1..=4).collect()) // provide some non-uniform stake
        .build();
    let committee: Committee = fixture.committee();

    let authorities = fixture.authorities().collect::<Vec<&AuthorityFixture>>();

    // The authority that creates the Header
    let authority = authorities[0];

    let header = Header::new(authority.id(), 1, 1, IndexMap::new(), Vec::new(), BTreeSet::new());

    // WHEN
    let mut votes: Vec<(AuthorityIdentifier, BlsSignature)> = Vec::new();
    let mut sorted_signers: Vec<BlsPublicKey> = Vec::new();

    // The authorities on position 1, 2, 3 are the ones who would sign
    for authority in &authorities[1..=3] {
        sorted_signers.push(authority.keypair().public().clone());

        let vote = Vote::new_with_signer(&header.clone(), &authority.id(), authority.keypair());
        votes.push((vote.author(), vote.signature().clone()));
    }

    // Just shuffle to ensure that any underlying sorting will work correctly
    votes.shuffle(&mut OsRng);

    // Create a certificate
    let certificate = Certificate::new_unverified(&committee, header, votes).unwrap();

    let (stake, signers) = certificate.signed_by(&committee);

    // THEN
    assert_eq!(signers.len(), 3);

    // AND authorities public keys are returned in order
    assert_eq!(signers, sorted_signers);

    assert_eq!(stake, 9 as Stake);
}
