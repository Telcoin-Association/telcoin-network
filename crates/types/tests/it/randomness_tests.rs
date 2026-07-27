//! Epoch-close committee-shuffle randomness tests (#1032).
//!
//! These tests pin the redesigned randomness derivation: the committee-shuffle seed comes from
//! the keccak hash of the LEADER'S INDIVIDUAL deterministic BLS signature over the canonical
//! per-epoch seed message, never from the certificate's aggregate signature (which varies with
//! the 2f+1 signer subset and let a Byzantine leader fork the shuffle).

use alloy::primitives::keccak256;
use std::num::NonZeroUsize;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{BlsSignature, Certificate, CommittedSubDag, Hash as _, ReputationScores};

/// THE pin for #1032: two certificates over the SAME header built from DIFFERENT 2f+1 vote
/// subsets must derive IDENTICAL committee-shuffle randomness.
///
/// Under the OLD code (randomness = keccak of the certificate's aggregate BLS signature) this
/// test would FAIL: the two 5-vote subsets aggregate to different signature bytes, so the two
/// certificates - identical header, identical digest, deduplicated as one by cert_validator -
/// derived two different shuffles, letting a Byzantine leader fork the epoch boundary.
#[tokio::test]
async fn test_randomness_identical_across_certificate_vote_subsets() {
    // Committee of 7: f = 2, quorum = 2f + 1 = 5. The author does not vote, leaving 6 votes to
    // carve two distinct 5-vote quorums from.
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(7).unwrap())
        .build();
    let committee = fixture.committee();

    // One header from a fixture authority (carries its valid seed signature).
    let header = fixture.header_from_last_authority();
    let votes: Vec<_> =
        fixture.votes(&header).into_iter().map(|v| (v.author().clone(), *v.signature())).collect();
    assert_eq!(votes.len(), 6, "6 voters for a 7-committee (author is skipped)");

    // Two DIFFERENT 5-vote subsets over the SAME header.
    let subset_a: Vec<_> = votes.iter().take(5).cloned().collect();
    let subset_b: Vec<_> = votes.iter().skip(1).take(5).cloned().collect();
    assert_ne!(subset_a, subset_b, "vote subsets must differ");

    let cert_a = Certificate::new_unverified(&committee, header.clone(), subset_a)
        .expect("5 votes reach quorum for a 7-committee");
    let cert_b = Certificate::new_unverified(&committee, header.clone(), subset_b)
        .expect("5 votes reach quorum for a 7-committee");

    // Same header-only digest (this is exactly why first-write-wins dedup can mix them)...
    assert_eq!(cert_a.digest(), cert_b.digest(), "certificate digest is header-only");
    // ...but different aggregate signatures - the old randomness source.
    assert_ne!(
        cert_a.aggregated_signature(),
        cert_b.aggregated_signature(),
        "different signer subsets must aggregate differently (the vulnerability this fixes)"
    );

    let sub_dag_a = CommittedSubDag::new(
        vec![cert_a.clone()],
        cert_a,
        1,
        ReputationScores::new(&committee),
        None,
    );
    let sub_dag_b = CommittedSubDag::new(
        vec![cert_b.clone()],
        cert_b,
        1,
        ReputationScores::new(&committee),
        None,
    );

    // Identical randomness regardless of which quorum certified the leader header...
    assert_eq!(
        sub_dag_a.randomness(),
        sub_dag_b.randomness(),
        "randomness must not depend on the certificate's signer subset"
    );
    // ...and exactly the keccak of the leader's seed signature.
    assert_eq!(
        sub_dag_a.randomness(),
        keccak256(header.seed_signature().to_bytes()),
        "randomness must be the keccak of the leader's seed signature"
    );
}

/// Grinding-resistance pin for #1032: headers from the same authority in the same epoch carry
/// the same seed signature no matter how their content differs, so a leader cannot grind
/// payloads, parents, or timestamps to steer the committee shuffle.
#[tokio::test]
async fn test_randomness_immune_to_header_content_grinding() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(7).unwrap())
        .build();
    let committee = fixture.committee();
    let authority = fixture.last_authority();

    // Two headers from the same authority + epoch differing in created_at and payload.
    let header_a = authority.header_builder(&committee).created_at(1).build();
    let header_b = authority
        .header_builder(&committee)
        .created_at(2)
        .payload([(Default::default(), 0)].into_iter().collect())
        .build();
    assert_ne!(header_a.digest(), header_b.digest(), "headers must differ");

    // Same deterministic seed signature - the message signs only (epoch, prior record).
    assert_eq!(
        header_a.seed_signature(),
        header_b.seed_signature(),
        "seed signature must not depend on header content"
    );

    // And thus identical committee-shuffle randomness through the full derivation.
    let cert_a = fixture.certificate(&header_a);
    let cert_b = fixture.certificate(&header_b);
    let sub_dag_a = CommittedSubDag::new(
        vec![cert_a.clone()],
        cert_a,
        1,
        ReputationScores::new(&committee),
        None,
    );
    let sub_dag_b = CommittedSubDag::new(
        vec![cert_b.clone()],
        cert_b,
        2,
        ReputationScores::new(&committee),
        None,
    );
    assert_eq!(
        sub_dag_a.randomness(),
        sub_dag_b.randomness(),
        "grinding header content must not move the shuffle seed"
    );
}

/// Totality pin for #1032: `CommittedSubDag::new` with a leader header carrying the DEFAULT (BLS
/// infinity) seed signature derives randomness without panicking, and the value is exactly the
/// keccak of the default signature's bytes.
///
/// This pins that the removed silent `unwrap_or_else(|| BlsSignature::default())` fallback stays
/// gone: the derivation is already total because every header carries a `seed_signature` field,
/// and it hashes *that* field - never the certificate's aggregate signature. Reintroducing the old
/// aggregate-based derivation would hash the cert's (non-default) aggregate here and break this.
#[tokio::test]
async fn test_randomness_from_default_seed_signature_is_total() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let authority = fixture.last_authority();

    // A header whose seed signature is the DEFAULT (BLS infinity) signature.
    let header =
        authority.header_builder(&committee).seed_signature(BlsSignature::default()).build();
    let cert = fixture.certificate(&header);
    let sub_dag =
        CommittedSubDag::new(vec![cert.clone()], cert, 1, ReputationScores::new(&committee), None);

    // No panic, and randomness is exactly the keccak of the default signature bytes - not the
    // certificate's (non-default) aggregate signature.
    assert_eq!(
        sub_dag.randomness(),
        keccak256(BlsSignature::default().to_bytes()),
        "randomness must be the keccak of the leader's (default) seed signature bytes"
    );
}
