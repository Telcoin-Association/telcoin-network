//! Consensus utils

use crate::consensus::ConsensusState;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
};
use tn_types::{
    forks, AuthorityIdentifier, Certificate, DefaultHashFunction, Hash as _, HeaderDigest, Round,
    B256, DIGEST_LENGTH,
};
use tracing::{debug, info};

/// Domain separator for the post-fork intra-round tie-break key (#1260).
///
/// Keeps [`intra_round_key`]'s preimage disjoint from every other derivation over the same
/// operands, matching the tagged siblings (`TN_EPOCH_SEED_ROOT_V1`, `TN_EPOCH_SEED_FOLD_V1`,
/// the `prev_randao` domain, `IntentScope::EpochCloseSeed`). Without it the 64-byte untagged
/// preimage would be a proper prefix of `CommittedSubDag::digest`'s own preimage for a
/// two-header sub-DAG.
const INTRA_ROUND_ORDER_DOMAIN: &[u8] = b"TN_INTRA_ROUND_ORDER_V1";

/// Flatten the dag referenced by the input certificate. This is a classic depth-first search
/// (pre-order): <https://en.wikipedia.org/wiki/Tree_traversal#Pre-order>
pub(crate) fn order_dag(leader: &Certificate, state: &ConsensusState) -> Vec<Certificate> {
    order_dag_inner(leader, state, forks::leader_seeded_ordering_active(leader.epoch()))
}

/// [`order_dag`] with the fork-gate decision injected as a parameter.
///
/// The gate cannot be flipped at runtime (the `test-utils` override latches in a process-wide
/// `OnceLock`), so tests reach both arms, and the boundary between them, through this seam
/// instead of skipping on builds where one arm is unreachable. Production callers go through
/// [`order_dag`], which derives the flag from the epoch carried inside the committed leader.
pub(crate) fn order_dag_inner(
    leader: &Certificate,
    state: &ConsensusState,
    leader_seeded: bool,
) -> Vec<Certificate> {
    debug!("Processing sub-dag of {:?}", leader);
    assert!(leader.round() > 0);
    let gc_round = leader.round().saturating_sub(state.gc_depth);

    let mut ordered = Vec::new();
    let mut already_ordered = HashSet::new();
    let mut skipped = SkippedParents::default();

    let mut buffer = vec![leader];
    while let Some(x) = buffer.pop() {
        debug!("Sequencing {:?}", x);
        ordered.push(x.clone());
        if x.round() == gc_round + 1 {
            // Do not try to order parents of the certificate, since they have been GC'ed.
            continue;
        }
        for parent in x.header().parents() {
            match state
                .dag
                .get(&(x.round() - 1))
                .and_then(|x| x.values().find(|(x, _)| x == parent))
            {
                Some((digest, certificate)) => {
                    // We skip the certificate if we (1) already processed it or (2) we reached a
                    // round that we already committed or will never commit for
                    // this authority.
                    let mut skip = already_ordered.contains(&digest);
                    skip |= state
                        .last_committed
                        .get(certificate.origin())
                        .map_or_else(|| false, |r| &certificate.round() <= r);
                    if skip {
                        skipped.note(certificate, &state.last_committed);
                    }
                    if !skip {
                        buffer.push(certificate);
                        already_ordered.insert(digest);
                    }
                }
                None => tracing::error!("Parent digest {parent:?} not found for {x:?}!"),
            }
        }
    }
    skipped.log(leader);

    // Ordering the output by round is necessary to make sure batches from early rounds go in before
    // later rounds and to put the leader at the end plus it makes the commit sequence prettier.
    // Note, the leader should be a single certificate with the highest round, it will sort last-
    // this marks it as the leader.
    //
    // Within a round, the DFS discovery order above is derived from the digest-ordered parent
    // sets, and a header digest is proposer-grindable (#1260): `Header::validate` does not
    // constrain the `payload` insertion order, so a proposer can permute it to steer its own
    // certificate toward either end of its round, and that position survives into execution
    // with no re-sort. Post-fork, the intra-round tie-break is keyed on the epoch seed chain
    // value this same commit is about to fold into `CommittedSubDag::randomness`
    // (`bullshark.rs` passes `state.seed_chain()` to `CommittedSubDag::new` immediately after
    // this call and only advances it afterwards, so both read the identical value). No
    // participant, the leader included, can enumerate candidate seeds after observing the
    // sub-DAG: the chain value is fixed by the previous published commit and the leader's
    // contribution is its deterministic BLS seed signature, one value per
    // `(author, epoch, round)`; `intra_round_key` documents the remaining last-actor
    // residual. The gate reads the epoch carried inside the leader itself, never node-local
    // state, so replay of a pre-fork commit reproduces the historical sequence.
    //
    // The seed is `None` on the legacy arm, and also when the leader header carries no seed
    // signature. The second case is unreachable while `leader_seeded_ordering_active`
    // conjoins `seed_signature_active`; degrading to the legacy rule rather than panicking
    // means a mis-set fork schedule holds the whole fleet on the pre-fork order uniformly
    // instead of halting it.
    let seed = leader_seeded
        .then(|| leader.header().seed_signature())
        .flatten()
        .map(|signature| state.seed_chain().fold(leader.round(), signature).into_inner());
    // The sort is stable, so a `(round, None)` key is exactly the legacy round-only sort: it
    // preserves the (grindable) DFS discovery order within each round.
    ordered.sort_by_cached_key(|x| {
        (x.round(), seed.as_ref().map(|seed| intra_round_key(seed, &x.digest())))
    });
    ordered
}

/// The post-fork intra-round tie-break for a committed sub-DAG:
/// `blake3(domain || seed || certificate_digest)` (#1260), where `seed` is the epoch seed
/// chain value this commit folds into `CommittedSubDag::randomness`.
///
/// The seed chain makes the key unpredictable to every proposer, the leader included: the
/// chain value is fixed by the previous published commit (every fold input is digest-pinned,
/// see `seed_chain.rs`), and the leader's own contribution is its deterministic BLS seed
/// signature, which admits exactly one value per `(author, epoch, round)`, so there is no
/// candidate set to grind. A round-`r` proposer cannot evaluate the seed while it builds its
/// header, because the preceding commit is not yet published, so grinding its own header
/// digest buys no chosen position. The residual that remains is the seed chain's accepted
/// one: the committing leader can evaluate the seed its commit would produce and withhold the
/// proposal, a one-shot propose-or-withhold choice, not a selection among millions of
/// candidate orders. The key is a pure function of committed data, so every honest node
/// derives the identical order. The leader still sorts last: it is the only certificate at
/// the highest round, so no tie-break applies to it.
fn intra_round_key(seed: &B256, certificate_digest: &HeaderDigest) -> [u8; DIGEST_LENGTH] {
    let mut hasher = DefaultHashFunction::new();
    hasher.update(INTRA_ROUND_ORDER_DOMAIN);
    hasher.update(seed.as_ref());
    hasher.update(certificate_digest.as_ref());
    hasher.finalize().into()
}

/// Calculates the GC round given a commit round and the gc_depth
pub fn gc_round(commit_round: Round, gc_depth: Round) -> Round {
    commit_round.saturating_sub(gc_depth)
}

/// The most `(origin, round)` entries the `skips` field of `subdag parent skipped` lists.
const MAX_LOGGED_SKIPS: usize = 10;

/// The parents [`order_dag_inner`] skips because their origin already committed a later round:
/// the fields of the `subdag parent skipped` line.
///
/// A parent at exactly its origin's last committed round is that origin's committed certificate
/// (the DAG holds one certificate per origin and round), so only a parent below it is noted: that
/// certificate was committed by an earlier sub-dag or is never committed. One sub-dag notes at most
/// one entry per certificate of its GC window and one edge per parent edge it walks.
#[derive(Debug, Default)]
struct SkippedParents {
    /// The last committed round of each skipped certificate's origin, keyed by the certificate's
    /// round and origin.
    certificates: BTreeMap<(Round, AuthorityIdentifier), Round>,
    /// The parent edges that hit such a skip: a certificate named by three children counts three.
    edges: usize,
}

impl SkippedParents {
    /// Note `certificate`, a parent the ordering skipped, if its round is below its origin's last
    /// committed round in `last_committed`.
    fn note(
        &mut self,
        certificate: &Certificate,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
    ) {
        let Some(&last) = last_committed.get(certificate.origin()) else {
            return;
        };
        if certificate.round() < last {
            self.edges += 1;
            self.certificates.insert((certificate.round(), certificate.origin().clone()), last);
        }
    }

    /// Write one `subdag parent skipped` line for the sub-dag of `leader`, if it skipped a parent
    /// below its origin's last committed round.
    fn log(&self, leader: &Certificate) {
        if self.certificates.is_empty() {
            return;
        }
        info!(
            target: "consensus::metrics",
            epoch = leader.epoch(),
            leader_round = leader.round(),
            skipped = self.certificates.len(),
            edges = self.edges,
            skips = %LoggedSkips(&self.certificates),
            "subdag parent skipped"
        );
    }
}

/// The `skips` field: the lowest [`MAX_LOGGED_SKIPS`] skipped certificates by round, then origin,
/// as `[origin:round:last_committed,…]` with the full origin id and no spaces.
struct LoggedSkips<'a>(&'a BTreeMap<(Round, AuthorityIdentifier), Round>);

impl fmt::Display for LoggedSkips<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, ((round, origin), last)) in self.0.iter().take(MAX_LOGGED_SKIPS).enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{origin}:{round}:{last}")?;
        }
        f.write_str("]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-layout pin for [`intra_round_key`] over a fixed input pair (#1260).
    ///
    /// The key is consensus-critical: every honest node must derive the identical intra-round
    /// order, so the exact preimage layout `domain || seed || certificate_digest` is part of
    /// the protocol. This pin is what catches an accidental operand swap, a dropped hasher
    /// update, or a silent domain-tag change in a later refactor; the property tests in
    /// `bullshark_tests.rs` cannot, because they compare the key only against itself.
    #[test]
    fn intra_round_key_byte_layout_is_pinned() {
        let seed = B256::repeat_byte(0x11);
        let digest = HeaderDigest::new([0x22; DIGEST_LENGTH]);
        let expected: [u8; DIGEST_LENGTH] = [
            0x67, 0xa3, 0xe5, 0xe0, 0xcf, 0xb8, 0x0b, 0x8a, 0xb6, 0xfe, 0x53, 0x14, 0x90, 0xf3,
            0xe1, 0xa1, 0xef, 0x4b, 0x54, 0xdb, 0x38, 0x8a, 0xd6, 0x93, 0xc8, 0x97, 0xae, 0x5c,
            0xdd, 0x43, 0xb4, 0x21,
        ];
        assert_eq!(
            intra_round_key(&seed, &digest),
            expected,
            "intra_round_key must remain blake3(TN_INTRA_ROUND_ORDER_V1 || seed || \
             certificate_digest); a mismatch means the preimage layout changed, which changes \
             every post-fork commit order",
        );
    }

    /// `subdag parent skipped` on a four-authority DAG of rounds 1 to 3, every certificate citing
    /// the whole round below, ordered from a round 3 leader. Nothing committed: no line. Then
    /// authority `a` has committed round 2 and `b` round 3: `a`'s round 2 certificate is skipped
    /// at its last commit without an entry, `b`'s round 2 certificate is one entry over one edge,
    /// and the round 1 certificates of both are named by each of the two round 2 certificates
    /// still ordered: two entries over four edges.
    ///
    /// Catches: a line per edge, an entry for a skip at the last committed round, a line for a
    /// sub-dag without such a skip, and a list in discovery order.
    #[test]
    fn subdag_parent_skipped_reports_parents_below_the_last_commit() {
        use crate::{log_capture::LogCapture, test_utils::make_optimal_certificates};
        use std::collections::BTreeSet;
        use tn_storage::mem_db::MemDatabase;
        use tn_test_utils_committee::CommitteeFixture;

        let capture = LogCapture::new("consensus::metrics");
        let _capture_guard = tracing::subscriber::set_default(capture.clone());
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let ids: Vec<_> = fixture.authorities().map(|authority| authority.id()).collect();
        let genesis: BTreeSet<_> =
            Certificate::genesis(&committee).iter().map(|cert| cert.digest()).collect();
        let (certificates, _) = make_optimal_certificates(&committee, 1..=3, &genesis, &ids);
        let mut state = ConsensusState::new(50);
        for certificate in &certificates {
            state.try_insert(certificate).expect("certificate inserts");
        }
        let leader = certificates.iter().find(|cert| cert.round() == 3).expect("a round 3 leader");

        order_dag_inner(leader, &state, false);
        assert!(capture.events("subdag parent skipped").is_empty(), "nothing committed");

        let others: Vec<AuthorityIdentifier> =
            ids.iter().filter(|id| *id != leader.origin()).cloned().collect();
        let (a, b) = (others[0].clone(), others[1].clone());
        state.last_committed.insert(a.clone(), 2);
        state.last_committed.insert(b.clone(), 3);
        let ordered = order_dag_inner(leader, &state, false);
        assert!(ordered.iter().all(|cert| cert.origin() != &a && cert.origin() != &b));

        let lines = capture.events("subdag parent skipped");
        assert_eq!(lines.len(), 1, "one line per sub-dag");
        let line = &lines[0];
        let names: Vec<&str> = line.fields.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["epoch", "leader_round", "skipped", "edges", "skips"]);
        assert_eq!(line.u64_field("epoch"), Some(u64::from(leader.epoch())));
        assert_eq!(line.u64_field("leader_round"), Some(3));
        assert_eq!(line.u64_field("skipped"), Some(3));
        assert_eq!(line.u64_field("edges"), Some(5));
        let expected = BTreeMap::from([((1, a.clone()), 2), ((1, b.clone()), 3), ((2, b), 3)]);
        let entries: Vec<String> = expected
            .iter()
            .map(|((round, origin), last)| format!("{origin}:{round}:{last}"))
            .collect();
        assert_eq!(line.field("skips"), Some(format!("[{}]", entries.join(",")).as_str()));
    }
}
