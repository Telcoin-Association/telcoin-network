//! Epoch-close committee-shuffle randomness tests (#1032).
//!
//! These tests pin the redesigned randomness derivation: the committee-shuffle seed folds the
//! LEADER'S INDIVIDUAL deterministic BLS signature over the canonical per-`(author, round)` seed
//! message into the epoch seed chain, never the certificate's aggregate signature (which varies
//! with the 2f+1 signer subset and let a Byzantine leader fork the shuffle).

use alloy::primitives::keccak256;
use std::num::NonZeroUsize;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    BlsSignature, Certificate, CommittedSubDag, Epoch, EpochSeedChainValue, EpochSeedMessage,
    Hash as _, ReputationScores, Round, B256,
};

/// The pinned fixture anchor, spelled out here instead of read back from
/// [`EpochSeedChainValue::genesis_placeholder`].
///
/// Every expectation below is built from this literal so that no assertion in this file can be
/// satisfied by the production constructor simply agreeing with itself.
const FIXTURE_ANCHOR: B256 = B256::ZERO;

/// The fold domain separator, duplicated here on purpose (see [`independent_fold`]).
const FOLD_DOMAIN: &[u8] = b"TN_EPOCH_SEED_FOLD_V1";

/// The root domain separator, duplicated here on purpose (see [`independent_root`]).
const ROOT_DOMAIN: &[u8] = b"TN_EPOCH_SEED_ROOT_V1";

/// Recompute one seed chain fold step INDEPENDENTLY of the production helper.
///
/// This deliberately does not call [`EpochSeedChainValue::fold`]. `CommittedSubDag::new` calls
/// that helper, so an expectation built with it would assert the code equals itself: every
/// mutation *inside* the fold (dropping the domain tag, dropping the round, dropping the previous
/// value, swapping the hash) would move both sides of the assertion together and stay green.
/// Spelling the preimage out here is what gives these assertions force.
fn independent_fold(anchor: B256, leader_round: Round, signature: &BlsSignature) -> B256 {
    let preimage: Vec<u8> = FOLD_DOMAIN
        .iter()
        .copied()
        .chain(anchor.as_slice().iter().copied())
        .chain(leader_round.to_le_bytes())
        .chain(signature.to_bytes())
        .collect();
    keccak256(preimage)
}

/// Recompute the per-epoch chain root INDEPENDENTLY of [`EpochSeedChainValue::epoch_root`], for
/// the same reason as [`independent_fold`].
fn independent_root(epoch: Epoch) -> B256 {
    let preimage: Vec<u8> = ROOT_DOMAIN.iter().copied().chain(epoch.to_le_bytes()).collect();
    keccak256(preimage)
}

/// The fixture anchor is a pinned constant, not a derived value.
///
/// Catches: changing [`EpochSeedChainValue::genesis_placeholder`] to anything other than the zero
/// word (in particular, deriving it from node-local epoch state, which would give every node a
/// different genesis anchor and fork the chain at its first link). This is also the pin that lets
/// the rest of this file build expectations from the [`FIXTURE_ANCHOR`] literal.
#[test]
fn test_genesis_placeholder_is_the_pinned_zero_anchor() {
    assert_eq!(
        EpochSeedChainValue::genesis_placeholder().into_inner(),
        FIXTURE_ANCHOR,
        "the genesis placeholder anchor must stay the pinned zero word forever"
    );
}

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
        EpochSeedChainValue::genesis_placeholder(),
    );
    let sub_dag_b = CommittedSubDag::new(
        vec![cert_b.clone()],
        cert_b,
        1,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );

    // Identical randomness regardless of which quorum certified the leader header...
    assert_eq!(
        sub_dag_a.randomness(),
        sub_dag_b.randomness(),
        "randomness must not depend on the certificate's signer subset"
    );
    // ...and exactly the anchor folded with the leader's round and seed signature, recomputed
    // here from literal domain bytes rather than by calling the production fold helper.
    assert_eq!(
        sub_dag_a.randomness(),
        independent_fold(FIXTURE_ANCHOR, header.round(), header.seed_signature()),
        "randomness must be the seed chain fold of the leader's round and seed signature"
    );
}

/// Grinding-resistance pin for #1032: headers from the same authority at the same `(epoch, round)`
/// carry the same seed signature no matter how their content differs, so a leader cannot grind
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

    // Same deterministic seed signature - the message signs only (epoch, round, prior record), and
    // both headers are at the builder's round 1.
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
        EpochSeedChainValue::genesis_placeholder(),
    );
    let sub_dag_b = CommittedSubDag::new(
        vec![cert_b.clone()],
        cert_b,
        2,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );
    assert_eq!(
        sub_dag_a.randomness(),
        sub_dag_b.randomness(),
        "grinding header content must not move the shuffle seed"
    );
}

/// Totality pin for #1032: `CommittedSubDag::new` with a leader header carrying the DEFAULT (BLS
/// infinity) seed signature derives randomness without panicking, and the value is exactly the
/// anchor folded with the leader's round and that default signature's bytes.
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
    let sub_dag = CommittedSubDag::new(
        vec![cert.clone()],
        cert,
        1,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );

    // No panic, and randomness is exactly the anchor folded with the leader's round and the default
    // signature bytes - not the certificate's (non-default) aggregate signature. The expectation is
    // recomputed here from literal domain bytes, never by calling the production fold helper.
    assert_eq!(
        sub_dag.randomness(),
        independent_fold(FIXTURE_ANCHOR, header.round(), &BlsSignature::default()),
        "randomness must fold the leader's (default) seed signature bytes"
    );
}

/// Root pin: the chain root for an epoch depends on the epoch number and NOTHING else.
///
/// Catches: feeding any node-local value back into the root, in particular the digest of the local,
/// uncertified [`EpochRecord`](tn_types::EpochRecord). The root reaches
/// `CommittedSubDag::randomness` and from there the executed block's `parent_beacon_block_root`, so
/// a node-local input would let one node's divergent or half-written storage fork execution on the
/// epoch's FIRST commit instead of merely costing that node its vote. Cross-epoch binding lives in
/// [`EpochSeedMessage`], which every voter verifies against its own view before signing, and it is
/// deliberately NOT duplicated here.
#[test]
fn test_epoch_root_depends_only_on_the_epoch() {
    let epoch: Epoch = 9;
    assert_eq!(
        EpochSeedChainValue::epoch_root(epoch).into_inner(),
        independent_root(epoch),
        "the epoch root must be the domain separated hash of the epoch alone"
    );
    assert_ne!(
        EpochSeedChainValue::epoch_root(epoch),
        EpochSeedChainValue::epoch_root(epoch + 1),
        "consecutive epochs must start from different roots"
    );
    assert_ne!(
        EpochSeedChainValue::epoch_root(epoch),
        EpochSeedChainValue::genesis_placeholder(),
        "a real epoch root must never collide with the fixture anchor"
    );
}

/// Chain pin: the randomness of a commit is a fold over the PREVIOUS commit of the same epoch, not
/// a per-epoch constant.
///
/// Two sequential committed sub-dags of one epoch are built - `C1` off the epoch root, `C2` off
/// `C1`'s chain value - and `randomness(C2)` is checked against a fold recomputed here from literal
/// domain bytes over `randomness(C1)`, `C2`'s leader round and `C2`'s leader seed signature.
///
/// Catches: reverting the derivation in `CommittedSubDag::new` to the old per-epoch constant
/// `keccak256(leader_seed_signature)`, which the explicit `assert_ne!` names directly. It also
/// catches dropping the previous value from the fold (the expectation would then match a root
/// anchored fold instead, which the second `assert_ne!` pins) and dropping the leader round.
#[tokio::test]
async fn test_fold_binds_previous_commit() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let epoch = committee.epoch();

    // The epoch root, cross-checked against an independently recomputed preimage so the rest of
    // this test can build on the production anchor without inheriting its derivation.
    let root = EpochSeedChainValue::epoch_root(epoch);
    assert_eq!(
        root.into_inner(),
        independent_root(epoch),
        "the epoch root must be the domain separated hash of the epoch alone"
    );

    // C1: the epoch's first commit, led by a round-1 header from one authority.
    let leader_1 = fixture.first_authority().header_with_round(&committee, 1);
    let cert_1 = fixture.certificate(&leader_1);
    let sub_dag_1 = CommittedSubDag::new(
        vec![cert_1.clone()],
        cert_1,
        0,
        ReputationScores::new(&committee),
        None,
        root,
    );

    // C2: the next commit of the same epoch, led by a round-2 header from a different authority,
    // anchored on C1 exactly the way bullshark threads the chain forward.
    let leader_2 = fixture.last_authority().header_with_round(&committee, 2);
    let cert_2 = fixture.certificate(&leader_2);
    assert_ne!(
        leader_1.seed_signature(),
        leader_2.seed_signature(),
        "the two leaders must contribute different seed signatures"
    );
    let sub_dag_2 = CommittedSubDag::new(
        vec![cert_2.clone()],
        cert_2,
        1,
        ReputationScores::new(&committee),
        Some(sub_dag_1.clone()),
        sub_dag_1.seed_chain_value(),
    );

    // C1 folds the epoch root.
    assert_eq!(
        sub_dag_1.randomness(),
        independent_fold(independent_root(epoch), leader_1.round(), leader_1.seed_signature()),
        "the epoch's first commit must fold the epoch root"
    );

    // C2 folds C1's value: this is the chain.
    assert_eq!(
        sub_dag_2.randomness(),
        independent_fold(sub_dag_1.randomness(), leader_2.round(), leader_2.seed_signature()),
        "a commit must fold the previous commit's chain value"
    );

    // ...and specifically NOT the old per-epoch constant.
    assert_ne!(
        sub_dag_2.randomness(),
        keccak256(leader_2.seed_signature().to_bytes()),
        "randomness must not be the keccak of the leader's seed signature alone"
    );

    // ...and not a re-rooted fold that ignores the previous commit.
    assert_ne!(
        sub_dag_2.randomness(),
        independent_fold(independent_root(epoch), leader_2.round(), leader_2.seed_signature()),
        "randomness must not re-root the chain at every commit"
    );

    assert_ne!(
        sub_dag_1.randomness(),
        sub_dag_2.randomness(),
        "the chain must advance from one commit to the next"
    );
}

/// Grinding-horizon pin: the seed message binds the header's round, so one authority's signature
/// for round `r` says nothing about its signature for round `r + 1`.
///
/// Catches: dropping the `round` field from `EpochSeedMessage`. That mutation leaves every other
/// test in this file green (the fold still hashes a round, headers at the same round still agree)
/// while silently restoring whole-epoch seed enumeration - any observer could compute every
/// authority's contribution for the entire epoch from a single round-1 header.
#[tokio::test]
async fn test_epoch_seed_message_binds_round() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let authority = fixture.last_authority();
    let config = authority.consensus_config();
    let public_key = authority.primary_public_key();
    let epoch = fixture.committee().epoch();
    let prior_epoch_record = config.prior_epoch_record();
    let round: Round = 7;

    // Same key, same epoch, same prior record; only the round differs.
    let message_r = EpochSeedMessage::new(epoch, round, prior_epoch_record);
    let message_r_next = EpochSeedMessage::new(epoch, round + 1, prior_epoch_record);
    let signature_r = message_r.sign(config.key_config());
    let signature_r_next = message_r_next.sign(config.key_config());

    assert_ne!(
        signature_r, signature_r_next,
        "consecutive rounds must produce different seed signatures"
    );

    // Each signature verifies against its own round's message...
    assert!(
        message_r.verify(&signature_r, &public_key),
        "a round's signature must verify against that round's message"
    );
    assert!(
        message_r_next.verify(&signature_r_next, &public_key),
        "a round's signature must verify against that round's message"
    );

    // ...and against no other round's message.
    assert!(
        !message_r.verify(&signature_r_next, &public_key),
        "the next round's signature must not verify against this round's message"
    );
    assert!(
        !message_r_next.verify(&signature_r, &public_key),
        "this round's signature must not verify against the next round's message"
    );
}
