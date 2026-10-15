//! Bounded, epoch-local observations of certificate authors and commit progress.
//!
//! A round is finalized at the first accepted certificate of a higher round, or thirty seconds
//! after its first certificate, whichever is serviced first. Late arrivals cannot revise the
//! result. Missing round numbers are not synthesized as failures.

use crate::{consensus::Dag, metrics::PrimaryMetrics};
use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tn_types::{AuthorityIdentifier, Committee, Epoch, Round, VotingPower};
use tokio::time::Instant;

#[cfg(test)]
#[path = "tests/liveness_tests.rs"]
mod tests;

/// Maximum local lateness window after a round's first accepted certificate.
pub(super) const QUORUM_OBSERVATION_WINDOW: Duration = Duration::from_secs(30);

/// One pending round, bounded by the active committee's distinct authorities.
struct RoundObservation {
    /// Round represented by this observation.
    round: Round,
    /// First arrival, never refreshed by duplicates or subsequent authors.
    first_seen: Instant,
    /// Committee authors already included in the voting power sum.
    authors: BTreeSet<AuthorityIdentifier>,
    /// Voting power of the distinct observed authors.
    voting_power: VotingPower,
}

impl RoundObservation {
    /// Include an authority's voting power at most once, including across equivocations.
    fn add_author(&mut self, author: &AuthorityIdentifier, voting_power: VotingPower) {
        if self.authors.insert(author.clone()) {
            self.voting_power += voting_power;
        }
    }
}

/// Owns one pending round and a monotonic watermark for one active consensus task and epoch.
///
/// Counters live for the process lifetime. The watermark starts at the highest recovered DAG
/// or committed round so partially replayed history cannot appear to have missing authors.
pub(super) struct RoundQuorumObserver {
    /// Committee supplying membership, epoch and the actual quorum rule.
    committee: Committee,
    /// Application-lifetime, label-free metric handles.
    metrics: PrimaryMetrics,
    /// Highest finalized or recovered round, excluding all older arrivals.
    evaluated_through: Round,
    /// The sole pending round, so even arbitrarily large round jumps use bounded state.
    pending: Option<RoundObservation>,
}

impl RoundQuorumObserver {
    /// Exclude all restored rounds, including uncommitted rounds with pre-existing authors.
    pub(super) fn from_recovered_dag(
        committee: Committee,
        metrics: PrimaryMetrics,
        committed_round: Round,
        dag: &Dag,
    ) -> Self {
        let recovered_round =
            dag.last_key_value().map(|(round, _)| *round).unwrap_or_default().max(committed_round);
        Self::new(committee, metrics, recovered_round)
    }

    /// Start an active observer after recovery, resetting epoch-local gauges and cursors.
    pub(super) fn new(
        committee: Committee,
        metrics: PrimaryMetrics,
        recovered_round: Round,
    ) -> Self {
        metrics.liveness_started_timestamp_seconds.set(unix_seconds());
        metrics.last_commit_timestamp_seconds.set(0.0);
        metrics.quorum_observation_active.set(1.0);
        Self { committee, metrics, evaluated_through: recovered_round, pending: None }
    }

    /// Observe an accepted current-epoch certificate, ignoring old rounds and nonmembers.
    ///
    /// Call only after the consensus protocol accepts the certificate. Evaluate expiry before
    /// incorporating the arrival, so a certificate at the deadline is already late.
    pub(super) fn observe(
        &mut self,
        epoch: Epoch,
        round: Round,
        author: &AuthorityIdentifier,
        now: Instant,
    ) {
        if epoch == self.committee.epoch() && round > self.evaluated_through {
            self.committee.authority(author).into_iter().for_each(|authority| {
                self.evaluate_expired(now);
                if round > self.evaluated_through {
                    if self.pending.as_ref().is_some_and(|pending| round > pending.round) {
                        self.finalize();
                    }
                    let pending = self.pending.get_or_insert_with(|| RoundObservation {
                        round,
                        first_seen: now,
                        authors: BTreeSet::new(),
                        voting_power: 0,
                    });
                    if pending.round == round {
                        pending.add_author(author, authority.voting_power());
                    }
                }
            });
        }
    }

    /// Finalize at the fixed deadline, including when no traffic arrives.
    pub(super) fn evaluate_expired(&mut self, now: Instant) {
        if self.pending.as_ref().is_some_and(|pending| {
            now.saturating_duration_since(pending.first_seen) >= QUORUM_OBSERVATION_WINDOW
        }) {
            self.finalize();
        }
    }

    /// Record successful handoff to the consensus output channel using local wall-clock time.
    pub(super) fn record_commit(&self) {
        self.metrics.last_commit_timestamp_seconds.set(unix_seconds());
    }

    /// Finalize once and release author state; future arrivals cannot reopen this round.
    fn finalize(&mut self) {
        self.pending.take().into_iter().for_each(|pending| {
            self.metrics.rounds_evaluated_total.increment(1);
            if pending.voting_power < self.committee.quorum_threshold() {
                self.metrics.round_quorum_shortfalls_total.increment(1);
            }
            self.evaluated_through = pending.round;
        });
    }
}

impl Drop for RoundQuorumObserver {
    /// Disable observation even if the consensus task errors or is cancelled during shutdown.
    fn drop(&mut self) {
        self.metrics.quorum_observation_active.set(0.0);
    }
}

/// Local wall-clock time for Prometheus comparisons; zero if the clock predates Unix.
fn unix_seconds() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}
