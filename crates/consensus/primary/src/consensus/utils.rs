//! Consensus utils

use crate::consensus::ConsensusState;
use std::collections::HashSet;
use tn_types::{
    forks, Certificate, DefaultHashFunction, Hash as _, HeaderDigest, Round, DIGEST_LENGTH,
};
use tracing::debug;

/// Flatten the dag referenced by the input certificate. This is a classic depth-first search
/// (pre-order): <https://en.wikipedia.org/wiki/Tree_traversal#Pre-order>
pub(crate) fn order_dag(leader: &Certificate, state: &ConsensusState) -> Vec<Certificate> {
    debug!("Processing sub-dag of {:?}", leader);
    assert!(leader.round() > 0);
    let gc_round = leader.round().saturating_sub(state.gc_depth);

    let mut ordered = Vec::new();
    let mut already_ordered = HashSet::new();

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
                    if !skip {
                        buffer.push(certificate);
                        already_ordered.insert(digest);
                    }
                }
                None => tracing::error!("Parent digest {parent:?} not found for {x:?}!"),
            }
        }
    }

    // Ordering the output by round is necessary to make sure batches from early rounds go in before
    // later rounds and to put the leader at the end plus it makes the commit sequence prettier.
    // Note, the leader should be a single certificate with the highest round, it will sort last-
    // this marks it as the leader.
    //
    // Within a round, the DFS discovery order above is derived from the digest-ordered parent
    // sets, and a header digest is proposer-grindable (#1260): `Header::validate` does not
    // constrain the `payload` insertion order, so a proposer can permute it to steer its own
    // certificate toward either end of its round, and that position survives into execution
    // with no re-sort. Post-fork, the intra-round tie-break is re-keyed on the committed
    // leader's digest. No other proposer knows that digest while it builds its own header, so
    // for every validator except the leader a position can no longer be ground in advance. The
    // schedule-determined leader still controls its own digest and so keeps a grind over its
    // sub-DAG's intra-round permutations on its own turn; `leader_seeded_key` documents why
    // that residual is accepted. The gate reads the epoch carried inside the leader
    // itself, never node-local state, so replay of a pre-fork commit reproduces the historical
    // sequence.
    if forks::leader_seeded_ordering_active(leader.epoch()) {
        let leader_digest = leader.digest();
        ordered.sort_by_cached_key(|x| (x.round(), leader_seeded_key(&leader_digest, &x.digest())));
    } else {
        // Legacy: a stable round-only sort, which preserves the grindable DFS order within each
        // round.
        ordered.sort_by_key(|x| x.round());
    }
    ordered
}

/// The post-fork intra-round tie-break for a committed sub-DAG:
/// `blake3(leader_digest || certificate_digest)` (#1260).
///
/// Seeding on the committed leader's digest makes the key unpredictable to every proposer
/// except the leader itself: a round-`r` proposer cannot know which certificate will lead the
/// commit that orders round `r`, so grinding its own header digest buys no chosen position.
/// The schedule-determined leader still controls the seed, so on its own turn it can grind its
/// digest to pick among reachable intra-round permutations of its sub-DAG. That residual is
/// accepted deliberately: it shrinks the attacker set from every proposer on every commit to
/// the one leader on its own turn, any seed that is a pure function of committed data keeps it
/// (the leader's header is the last input to be fixed), and removing it needs unpredictable
/// post-commit randomness, which the protocol does not have at linearization time. The key is
/// a pure function of committed data, so every honest node derives the identical order. The
/// leader still sorts last: it is the only certificate at the highest round, so no tie-break
/// applies to it.
fn leader_seeded_key(
    leader_digest: &HeaderDigest,
    certificate_digest: &HeaderDigest,
) -> [u8; DIGEST_LENGTH] {
    let mut hasher = DefaultHashFunction::new();
    hasher.update(leader_digest.as_ref());
    hasher.update(certificate_digest.as_ref());
    hasher.finalize().into()
}

/// Calculates the GC round given a commit round and the gc_depth
pub fn gc_round(commit_round: Round, gc_depth: Round) -> Round {
    commit_round.saturating_sub(gc_depth)
}
