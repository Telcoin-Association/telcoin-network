//! Helpers for generating consensus data.

use crate::CommitteeFixture;
use indexmap::IndexMap;
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    ops::RangeInclusive,
};
use tn_types::{
    now, test_chain_spec_arc, AuthorityIdentifier, Batch, BlockHash, Certificate,
    CertificateDigest, Database, Hash as _, HeaderBuilder, Round, WorkerId,
};

/// Create a random number of batches with signed transactions.
/// Caller's responsibility to ensure random accounts have balances.
fn random_batches(
    number_of_batches: usize,
) -> (IndexMap<BlockHash, WorkerId>, HashMap<BlockHash, Batch>) {
    let mut payload: IndexMap<BlockHash, WorkerId> = IndexMap::with_capacity(number_of_batches);
    let mut batches = HashMap::with_capacity(number_of_batches);

    let chain = test_chain_spec_arc();
    for _ in 0..number_of_batches {
        let batch = tn_reth::test_utils::batch(chain.clone());
        let batch_digest = batch.digest();

        payload.insert(batch_digest, 0);
        batches.insert(batch_digest, batch);
    }

    (payload, batches)
}

/// Creates one signed certificate from a set of signers - the signers must include the
/// origin/author.
fn signed_cert<DB>(
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    committee: &CommitteeFixture<DB>,
) -> (CertificateDigest, Certificate, HashMap<BlockHash, Batch>)
where
    DB: Database,
{
    let (payload, batches) = random_batches(3);
    let header = HeaderBuilder::default()
        .author(origin)
        .payload(payload)
        .round(round)
        .epoch(0)
        .parents(parents)
        .created_at(now())
        .build();

    let cert = committee.certificate(&header);
    (cert.digest(), cert, batches)
}

/// Creates one signed certificate with an empty payload (no batches).
fn signed_cert_empty<DB>(
    origin: AuthorityIdentifier,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
    committee: &CommitteeFixture<DB>,
) -> (CertificateDigest, Certificate)
where
    DB: Database,
{
    let header = HeaderBuilder::default()
        .author(origin)
        .payload(IndexMap::new())
        .round(round)
        .epoch(0)
        .parents(parents)
        .created_at(now())
        .build();

    let cert = committee.certificate(&header);
    (cert.digest(), cert)
}

/// Create a range of certificates for specified rounds from committee.
///
/// Rounds in `empty_rounds` generate certificates with empty payloads.
pub fn create_signed_certificates_for_rounds<DB>(
    range: RangeInclusive<Round>,
    fixture: &CommitteeFixture<DB>,
    empty_rounds: &[Round],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>, HashMap<BlockHash, Batch>)
where
    DB: Database,
{
    let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
    let mut certificates = VecDeque::new();
    let mut next_parents = BTreeSet::new();
    let mut batches = HashMap::new();
    // use genesis for initial parents
    let mut parents: BTreeSet<_> = fixture.genesis().collect();

    // create signed certificates for every round
    for round in range {
        next_parents.clear();
        let is_empty = empty_rounds.contains(&round);
        for id in &ids {
            if is_empty {
                let (digest, certificate) =
                    signed_cert_empty(id.clone(), round, parents.clone(), fixture);
                certificates.push_back(certificate);
                next_parents.insert(digest);
            } else {
                let (digest, certificate, payload) =
                    signed_cert(id.clone(), round, parents.clone(), fixture);
                certificates.push_back(certificate);
                next_parents.insert(digest);
                batches.extend(payload);
            }
        }
        parents.clone_from(&next_parents);
    }

    (certificates, next_parents, batches)
}

/// Create a range of certificates where rounds in `empty_rounds` have empty payloads.
///
/// This wrapper is kept for readability in tests that model mixed empty/non-empty consensus
/// rounds.
pub fn create_signed_certificates_with_empty_rounds<DB>(
    range: RangeInclusive<Round>,
    fixture: &CommitteeFixture<DB>,
    empty_rounds: &[Round],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>, HashMap<BlockHash, Batch>)
where
    DB: Database,
{
    create_signed_certificates_for_rounds(range, fixture, empty_rounds)
}
