use indexmap::IndexMap;
use std::{collections::BTreeSet, num::NonZeroUsize};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    AuthorityIdentifier, Certificate, CommittedSubDag, HeaderBuilder, ReputationScores,
};

#[test]
fn test_zero_timestamp_in_sub_dag() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();

    let header_builder = HeaderBuilder::default();
    let header = header_builder
        .author(AuthorityIdentifier::default())
        .round(2)
        .epoch(0)
        .created_at(50)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

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
        .author(AuthorityIdentifier::default())
        .round(2)
        .epoch(0)
        .created_at(newer_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

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
        .author(AuthorityIdentifier::default())
        .round(4)
        .epoch(0)
        .created_at(older_timestamp)
        .payload(IndexMap::new())
        .parents(BTreeSet::new())
        .build();

    let certificate = Certificate::new_unsigned_for_test(&committee, header, Vec::new()).unwrap();

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

    let mut ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

    // adding some scores
    scores.add_score(ids.first().unwrap(), 0);
    scores.add_score(ids.get(1).unwrap(), 10);
    scores.add_score(ids.get(2).unwrap(), 10);
    scores.add_score(ids.get(3).unwrap(), 10);
    scores.add_score(ids.get(4).unwrap(), 10);
    scores.add_score(ids.get(5).unwrap(), 20);
    scores.add_score(ids.get(6).unwrap(), 30);
    scores.add_score(ids.get(7).unwrap(), 30);
    scores.add_score(ids.get(8).unwrap(), 40);
    scores.add_score(ids.get(9).unwrap(), 40);

    // the expected authorities
    let expected_authorities = vec![
        (ids.pop().unwrap(), 40),
        (ids.pop().unwrap(), 40),
        (ids.pop().unwrap(), 30),
        (ids.pop().unwrap(), 30),
        (ids.pop().unwrap(), 20),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 10),
        (ids.pop().unwrap(), 0),
    ];

    // sorting the authorities
    let sorted_authorities = scores.authorities_by_score_desc();
    assert_eq!(sorted_authorities, expected_authorities);
}
