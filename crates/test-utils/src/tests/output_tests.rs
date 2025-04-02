use indexmap::IndexMap;
use std::{collections::BTreeSet, num::NonZeroUsize};
use tn_storage::mem_db::MemDatabase;
use tn_types::{
    AuthorityIdentifier, Certificate, CommittedSubDag, HeaderBuilder, ReputationScores,
};

use crate::CommitteeFixture;

#[test]
fn test_zero_timestamp_in_sub_dag() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(2)
        .epoch(0)
        .created_at(50)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

    // AND we initialise the sub dag via the "restore" way
    let sub_dag_round = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
    );

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round.commit_timestamp(), 50);
}

#[test]
fn test_monotonically_incremented_commit_timestamps() {
    // Create a certificate (leader) of round 2 with a high timestamp
    let newer_timestamp = 100;
    let older_timestamp = 50;

    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(2)
        .epoch(0)
        .created_at(newer_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

    // AND
    let sub_dag_round_2 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        1,
        ReputationScores::default(),
        None,
    );

    // AND commit timestamp is the leader's timestamp
    assert_eq!(sub_dag_round_2.commit_timestamp(), newer_timestamp);

    // Now create the leader of round 4 with the older timestamp
    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier(1u16))
        .round(4)
        .epoch(0)
        .created_at(older_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

    // WHEN create the sub dag based on the "previously committed" sub dag.
    let sub_dag_round_4 = CommittedSubDag::new(
        vec![certificate.clone()],
        certificate,
        2,
        ReputationScores::default(),
        Some(&sub_dag_round_2),
    );

    // THEN the latest sub dag should have the highest committed timestamp - basically the
    // same as the previous commit round
    assert_eq!(sub_dag_round_4.commit_timestamp(), sub_dag_round_2.commit_timestamp());
}

#[test]
fn test_authority_sorting_in_reputation_scores() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(10).unwrap())
        .build();
    let committee = fixture.committee();

    let mut scores = ReputationScores::new(&committee);

    let ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // adding some scores
    scores.add_score(ids[0], 0);
    scores.add_score(ids[1], 10);
    scores.add_score(ids[2], 10);
    scores.add_score(ids[3], 10);
    scores.add_score(ids[4], 10);
    scores.add_score(ids[5], 20);
    scores.add_score(ids[6], 30);
    scores.add_score(ids[7], 30);
    scores.add_score(ids[8], 40);
    scores.add_score(ids[9], 40);

    // the expected authorities
    let expected_authorities = vec![
        (ids[9], 40),
        (ids[8], 40),
        (ids[7], 30),
        (ids[6], 30),
        (ids[5], 20),
        (ids[4], 10),
        (ids[3], 10),
        (ids[2], 10),
        (ids[1], 10),
        (ids[0], 0),
    ];

    // sorting the authorities
    let sorted_authorities = scores.authorities_by_score_desc();
    assert_eq!(sorted_authorities, expected_authorities);
}
