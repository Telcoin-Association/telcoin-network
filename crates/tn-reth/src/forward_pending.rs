//! Bounded observer forwarding state, retained until canonical inclusion or expiry.
//!
//! An RPC acknowledgement starts an inclusion window. Only distinct consensus outputs newer than
//! the submission's canonical head advance that window. Retries remember accepting validators;
//! transport failures can still use the ordinary fallback walk. The pool owns this state so an
//! epoch's forwarder can be replaced without forgetting outstanding transactions.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    num::NonZeroU64,
    time::{Duration, Instant},
};

/// Default number of subsequently executed consensus outputs before another submission.
pub(crate) const DEFAULT_RETRY_OUTPUTS: u64 = 3;

/// Atomic canonical head snapshot at submission, including the output that is already executing.
#[derive(Clone, Debug)]
pub(crate) struct SubmissionHead<Output> {
    /// Latest canonical execution block at admission.
    number: u64,
    /// Consensus output shared by every block produced from that output.
    output: Option<Output>,
}

impl<Output> SubmissionHead<Output> {
    /// Capture the number and output identity from the same canonical header.
    pub(crate) fn new(number: u64, output: Option<Output>) -> Self {
        Self { number, output }
    }
}

/// Capacity shared by retained payloads and the validator identities needed for retry routing.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetentionLimits {
    /// Maximum retained transaction count, taken from the worker's pending-pool configuration.
    transactions: usize,
    /// Maximum retained payload and routing bytes, taken from the same configuration.
    bytes: usize,
}

impl RetentionLimits {
    /// Use the pending pool's existing count and byte limits for the forwarding buffer as well.
    pub(crate) fn new(transactions: usize, bytes: usize) -> Self {
        Self { transactions, bytes }
    }
}

/// What a retained transaction is waiting for.
#[derive(Debug)]
enum PendingState {
    /// A forward task owns this submission until its existing batch budget elapses.
    Sending(Instant),
    /// An RPC accepted the transaction; subsequent outputs must establish lack of inclusion.
    AwaitingInclusion(u64),
    /// The inclusion window or an interrupted send has elapsed.
    RetryDue,
    /// Pool insertion is in flight; a cancelled insertion can be reclaimed after this deadline.
    Reinserting(Instant),
    /// The transaction has been returned to the pool and awaits the next batch admission.
    Queued,
    /// Routing memory is full; retain inclusion tracking without sending more copies.
    ObserveOnly,
}

/// One payload and its retry history. Re-admission never refreshes its overall lifetime.
#[derive(Debug)]
struct PendingTransaction<Validator, Output> {
    /// The signed bytes needed if a later output makes a retry eligible.
    bytes: Vec<u8>,
    /// Validators that have already acknowledged this transaction.
    accepted_by: BTreeSet<Validator>,
    /// Canonical block and output at the most recent submission's admission.
    submitted_at: SubmissionHead<Output>,
    /// The single lifetime bound, independent of acknowledgements and retries.
    expires_at: Instant,
    /// Existing batch-prune grace, also respected when outputs arrive immediately after admission.
    retry_not_before: Instant,
    /// Ownership lease for asynchronous pool insertion, bounded by the original send budget.
    insertion_budget: Duration,
    /// Current owner of the transaction's next action.
    state: PendingState,
}

/// Pool-owned, bounded forwarding state. Callers serialize access and perform I/O outside its lock.
#[derive(Debug)]
pub(crate) struct PendingForwards<Hash, Validator, Output> {
    /// Outstanding transactions indexed by their signed transaction hash.
    entries: BTreeMap<Hash, PendingTransaction<Validator, Output>>,
    /// Payload and validator-identity bytes currently retained.
    retained_bytes: usize,
    /// Hard admission bounds shared by every clone of the worker's pool.
    limits: RetentionLimits,
    /// Number of distinct newer consensus outputs required after an acknowledgement.
    retry_outputs: u64,
    /// Last canonical block notification processed, for duplicate and stale-event suppression.
    latest_block: u64,
    /// Consensus output represented by that block; several execution blocks may share it.
    latest_output: Option<Output>,
}

impl<Hash: Ord + Clone, Validator: Ord + Clone, Output: Eq>
    PendingForwards<Hash, Validator, Output>
{
    /// Construct an empty buffer with the pool's configured limits.
    pub(crate) fn new(limits: RetentionLimits) -> Self {
        Self {
            entries: BTreeMap::new(),
            retained_bytes: 0,
            limits,
            retry_outputs: DEFAULT_RETRY_OUTPUTS,
            latest_block: 0,
            latest_output: None,
        }
    }

    /// Configure future inclusion windows. Zero-output speculative retransmission is disallowed.
    pub(crate) fn set_retry_outputs(&mut self, outputs: NonZeroU64) {
        self.retry_outputs = outputs.get();
    }

    /// Atomically reserve an entire batch before the caller permits the builder to prune it.
    ///
    /// Already outstanding copies are suppressed. Only new transactions and queued retries are
    /// returned to the sender. A refusal leaves every entry unchanged, so the caller keeps its
    /// batch.
    pub(crate) fn admit(
        &mut self,
        transactions: Vec<(Hash, Vec<u8>)>,
        head: SubmissionHead<Output>,
        now: Instant,
        send_budget: Duration,
        lifetime: Duration,
        prune_grace: Duration,
    ) -> Option<Vec<Vec<u8>>>
    where
        Output: Clone,
    {
        let unique: BTreeMap<_, _> = transactions.iter().map(|(hash, tx)| (hash, tx)).collect();
        let additions = unique.iter().filter(|(hash, _)| !self.entries.contains_key(*hash));
        let (count, bytes) = additions.fold((0_usize, 0_usize), |(count, bytes), (_, tx)| {
            (count.saturating_add(1), bytes.saturating_add(tx.len()))
        });
        (self.entries.len().saturating_add(count) <= self.limits.transactions
            && self.retained_bytes.saturating_add(bytes) <= self.limits.bytes)
            .then(|| {
                transactions
                    .into_iter()
                    .filter_map(|(hash, bytes)| {
                        let entry = self.entries.entry(hash).or_insert_with(|| {
                            self.retained_bytes += bytes.len();
                            PendingTransaction {
                                bytes: bytes.clone(),
                                accepted_by: BTreeSet::new(),
                                submitted_at: head.clone(),
                                expires_at: now + lifetime,
                                retry_not_before: now + prune_grace,
                                insertion_budget: send_budget,
                                state: PendingState::Queued,
                            }
                        });
                        matches!(entry.state, PendingState::Queued | PendingState::Reinserting(_))
                            .then(|| {
                                entry.submitted_at = head.clone();
                                entry.retry_not_before = now + prune_grace;
                                entry.state = PendingState::Sending(now + send_budget);
                                bytes
                            })
                    })
                    .collect()
            })
    }

    /// Return accepting validators to exclude from the next fallback walk.
    pub(crate) fn accepted_by(&self, hash: &Hash) -> BTreeSet<Validator> {
        self.entries.get(hash).map(|entry| entry.accepted_by.clone()).unwrap_or_default()
    }

    /// Start an inclusion window after one successful RPC acknowledgement.
    pub(crate) fn accepted(&mut self, hash: &Hash, validator: Validator) {
        self.entries.get_mut(hash).into_iter().for_each(|entry| {
            let additional =
                if entry.accepted_by.contains(&validator) { 0 } else { size_of::<Validator>() };
            entry.state = if self.retained_bytes.saturating_add(additional) <= self.limits.bytes {
                entry.accepted_by.insert(validator.clone());
                self.retained_bytes += additional;
                PendingState::AwaitingInclusion(self.retry_outputs)
            } else {
                PendingState::ObserveOnly
            };
        });
    }

    /// Preserve an earlier acknowledgement when a later walk cannot reach a fresh validator.
    pub(crate) fn defer(&mut self, hash: &Hash) {
        self.entries.get_mut(hash).into_iter().for_each(|entry| {
            entry.state = PendingState::AwaitingInclusion(self.retry_outputs);
        });
    }

    /// Remove an included, expired, or definitively invalid transaction and release its capacity.
    pub(crate) fn remove(&mut self, hash: &Hash) {
        self.entries.remove(hash).into_iter().for_each(|entry| {
            self.retained_bytes -=
                entry.bytes.len() + entry.accepted_by.len() * size_of::<Validator>();
        });
    }

    /// Consume an actual canonical notification, never the builder's provisional pool update.
    ///
    /// Missed notifications delay retries instead of fabricating progress. The caller checks local
    /// canonical inclusion again before requeueing, covering mined notifications that were missed.
    pub(crate) fn committed(
        &mut self,
        block: u64,
        output: Output,
        included: impl IntoIterator<Item = Hash>,
    ) where
        Output: Clone,
    {
        included.into_iter().for_each(|hash| self.remove(&hash));
        let fresh = block > self.latest_block && self.latest_output.as_ref() != Some(&output);
        self.latest_block = self.latest_block.max(block);
        if fresh {
            self.latest_output = Some(output.clone());
            self.entries.values_mut().for_each(|entry| match &mut entry.state {
                PendingState::AwaitingInclusion(remaining) => {
                    if block > entry.submitted_at.number
                        && entry.submitted_at.output.as_ref() != Some(&output)
                    {
                        *remaining = remaining.saturating_sub(1);
                        if *remaining == 0 {
                            entry.state = PendingState::RetryDue;
                        }
                    }
                }
                PendingState::Sending(_)
                | PendingState::Reinserting(_)
                | PendingState::RetryDue
                | PendingState::Queued
                | PendingState::ObserveOnly => {}
            });
        }
    }

    /// Requeue a bounded number of eligible transactions and expire old entries.
    ///
    /// A cancelled epoch task is recovered after its send budget even if the chain is quiet. An
    /// acknowledged transaction requires output progress, so a stalled chain causes no retry storm.
    pub(crate) fn ready(&mut self, now: Instant, limit: usize) -> (Vec<(Hash, Vec<u8>)>, usize) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| now >= entry.expires_at)
            .map(|(hash, _)| hash.clone())
            .collect();
        let expired_count = expired.len();
        expired.into_iter().for_each(|hash| self.remove(&hash));
        let ready =
            self.entries
                .iter_mut()
                .filter(|(_, entry)| {
                    now >= entry.retry_not_before
                        && match entry.state {
                            PendingState::Sending(deadline)
                            | PendingState::Reinserting(deadline) => now >= deadline,
                            PendingState::RetryDue => true,
                            PendingState::AwaitingInclusion(_)
                            | PendingState::Queued
                            | PendingState::ObserveOnly => false,
                        }
                })
                .take(limit)
                .map(|(hash, entry)| {
                    entry.state = PendingState::Reinserting(now + entry.insertion_budget);
                    (hash.clone(), entry.bytes.clone())
                })
                .collect();
        (ready, expired_count)
    }

    /// Keep a due retry recoverable when the local canonical lookup temporarily fails.
    pub(crate) fn retry_later(&mut self, hash: &Hash) {
        self.entries.get_mut(hash).into_iter().for_each(|entry| {
            entry.state = PendingState::RetryDue;
        });
    }

    /// Begin an asynchronous pool insertion without losing ownership on cancellation.
    pub(crate) fn reinserting(&mut self, hash: &Hash, now: Instant) {
        self.entries.get_mut(hash).into_iter().for_each(|entry| {
            entry.state = PendingState::Reinserting(now + entry.insertion_budget);
        });
    }

    /// Finish insertion without overwriting a concurrent batch admission or acknowledgement.
    pub(crate) fn reinserted(&mut self, hash: &Hash, present: bool) {
        if self
            .entries
            .get(hash)
            .is_some_and(|entry| matches!(entry.state, PendingState::Reinserting(_)))
        {
            if present {
                self.entries
                    .get_mut(hash)
                    .into_iter()
                    .for_each(|entry| entry.state = PendingState::Queued);
            } else {
                self.remove(hash);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed-width identities make routing-byte accounting observable without network adapters.
    type Pending = PendingForwards<u64, u8, u64>;
    /// Test send budget, advanced explicitly rather than by sleeping.
    const SEND: Duration = Duration::from_secs(10);
    /// Test retention lifetime, independent of the send budget.
    const LIFETIME: Duration = Duration::from_secs(100);
    /// The same ordering grace used for returning a batch's transactions to its pool.
    const GRACE: Duration = Duration::from_secs(1);

    /// Admit one signed payload with explicit canonical head and time.
    fn admit(pending: &mut Pending, hash: u64, head: u64, now: Instant) -> Option<Vec<Vec<u8>>> {
        pending.admit(
            vec![(hash, vec![42])],
            SubmissionHead::new(head, Some(head)),
            now,
            SEND,
            LIFETIME,
            GRACE,
        )
    }

    /// Normal inclusion releases retained capacity and prevents any later retry.
    #[test]
    fn included_transactions_never_retry() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(1, 8));
        assert_eq!(admit(&mut pending, 1, 0, now), Some(vec![vec![42]]));
        pending.accepted(&1, 10);
        pending.committed(1, 1, [1]);
        (2..=5).for_each(|block| pending.committed(block, block, []));
        assert_eq!(pending.ready(now + SEND, 10), (vec![], 0));
        assert_eq!(admit(&mut pending, 2, 5, now + SEND), Some(vec![vec![42]]));
    }

    /// Catch-up notifications, repeated blocks, and multiple blocks per output cannot shorten N.
    #[test]
    fn retries_require_three_distinct_new_outputs() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        assert!(pending
            .admit(
                vec![(1, vec![42])],
                SubmissionHead::new(100, Some(10)),
                now,
                SEND,
                LIFETIME,
                GRACE
            )
            .is_some());
        pending.accepted(&1, 10);
        pending.committed(99, 9, []);
        // The notification for block 100 was missed. Block 101 still belongs to its output.
        pending.committed(101, 10, []);
        pending.committed(102, 11, []);
        pending.committed(102, 11, []);
        pending.committed(103, 11, []);
        pending.committed(104, 12, []);
        assert!(pending.ready(now + GRACE, 10).0.is_empty());
        pending.committed(105, 13, []);
        assert_eq!(pending.ready(now + GRACE, 10).0, vec![(1, vec![42])]);
        assert!(pending.ready(now + GRACE, 10).0.is_empty());
    }

    /// Retry routing survives re-admission and excludes both earlier false acknowledgements.
    #[test]
    fn retries_remember_multiple_accepting_validators() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        pending.set_retry_outputs(NonZeroU64::MIN);
        assert!(admit(&mut pending, 1, 0, now).is_some());
        pending.accepted(&1, 10);
        pending.committed(1, 1, []);
        assert_eq!(pending.ready(now + GRACE, 10).0.len(), 1);
        pending.reinserted(&1, true);
        assert_eq!(admit(&mut pending, 1, 1, now + GRACE), Some(vec![vec![42]]));
        assert_eq!(pending.accepted_by(&1), BTreeSet::from([10]));
        pending.accepted(&1, 20);
        pending.committed(2, 2, []);
        assert_eq!(pending.ready(now + GRACE * 2, 10).0.len(), 1);
        pending.reinserted(&1, true);
        assert_eq!(admit(&mut pending, 1, 2, now + GRACE * 2), Some(vec![vec![42]]));
        assert_eq!(pending.accepted_by(&1), BTreeSet::from([10, 20]));
        pending.defer(&1);
        pending.committed(3, 3, [1]);
        assert!(pending.ready(now + SEND, 10).0.is_empty());
    }

    /// Admission applies count and byte bounds atomically, including duplicate hashes in a batch.
    #[test]
    fn capacity_refusal_keeps_the_entire_batch_unclaimed() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(1, 4));
        assert!(pending
            .admit(
                vec![(1, vec![1]), (2, vec![2])],
                SubmissionHead::new(0, Some(0)),
                now,
                SEND,
                LIFETIME,
                GRACE
            )
            .is_none());
        assert!(pending
            .admit(
                vec![(1, vec![1; 5])],
                SubmissionHead::new(0, Some(0)),
                now,
                SEND,
                LIFETIME,
                GRACE
            )
            .is_none());
        assert_eq!(
            pending.admit(
                vec![(1, vec![1; 4]), (1, vec![1; 4])],
                SubmissionHead::new(0, Some(0)),
                now,
                SEND,
                LIFETIME,
                GRACE
            ),
            Some(vec![vec![1; 4]])
        );
        assert_eq!(admit(&mut pending, 1, 0, now), Some(vec![]));
        pending.remove(&1);
        assert!(admit(&mut pending, 2, 0, now).is_some());
    }

    /// A full routing budget cannot drop history and resend to the same acknowledging validator.
    #[test]
    fn routing_memory_is_bounded_without_forgetting_a_censor() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 1));
        assert!(admit(&mut pending, 1, 0, now).is_some());
        pending.accepted(&1, 10);
        (1..=4).for_each(|block| pending.committed(block, block, []));
        assert!(pending.ready(now + SEND, 10).0.is_empty());
        assert_eq!(pending.ready(now + LIFETIME, 10), (vec![], 1));
        assert!(admit(&mut pending, 2, 4, now + LIFETIME).is_some());
    }

    /// Neither repeated client submissions nor retry admissions extend the retention lifetime.
    #[test]
    fn retries_and_duplicates_do_not_refresh_expiry() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        assert!(admit(&mut pending, 1, 0, now).is_some());
        pending.accepted(&1, 10);
        assert_eq!(admit(&mut pending, 1, 0, now + SEND), Some(vec![]));
        (1..=3).for_each(|block| pending.committed(block, block, []));
        assert_eq!(pending.ready(now + SEND, 10).0.len(), 1);
        pending.reinserted(&1, true);
        assert_eq!(admit(&mut pending, 1, 3, now + SEND), Some(vec![vec![42]]));
        pending.accepted(&1, 20);
        assert_eq!(pending.ready(now + LIFETIME, 10), (vec![], 1));
        assert!(pending.accepted_by(&1).is_empty());
    }

    /// Epoch task cancellation is recoverable, while an acknowledged transaction waits for output.
    #[test]
    fn interrupted_sends_recover_but_stalled_inclusion_does_not_retry() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        assert!(admit(&mut pending, 1, 0, now).is_some());
        assert!(admit(&mut pending, 2, 0, now).is_some());
        pending.accepted(&2, 10);
        assert!(pending.ready(now + GRACE, 10).0.is_empty());
        assert_eq!(pending.ready(now + SEND, 10).0, vec![(1, vec![42])]);
        pending.retry_later(&1);
        assert_eq!(pending.ready(now + SEND, 1).0, vec![(1, vec![42])]);
        pending.reinserted(&1, true);
        assert!(pending.ready(now + SEND * 2, 10).0.is_empty());
        assert_eq!(pending.ready(now + LIFETIME, 10), (vec![], 2));
    }

    /// Cancellation during insertion stays recoverable, and a concurrent admission keeps ownership.
    #[test]
    fn interrupted_insertions_recover_without_overwriting_a_new_send() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        assert_eq!(admit(&mut pending, 1, 0, now), Some(vec![vec![42]]));
        pending.reinserting(&1, now + GRACE);
        assert!(pending.ready(now + SEND, 10).0.is_empty());
        assert_eq!(pending.ready(now + SEND + GRACE, 10).0, vec![(1, vec![42])]);
        assert_eq!(admit(&mut pending, 1, 1, now + SEND + GRACE), Some(vec![vec![42]]));
        pending.reinserted(&1, true);
        assert_eq!(admit(&mut pending, 1, 1, now + SEND + GRACE), Some(vec![]));
        pending.accepted(&1, 10);
        pending.reinserted(&1, false);
        assert_eq!(pending.accepted_by(&1), BTreeSet::from([10]));
        assert!(pending.ready(now + SEND * 3, 10).0.is_empty());
    }

    /// Fast outputs cannot make a requeue race the existing batch-prune grace.
    #[test]
    fn retry_respects_prune_grace_and_maintenance_batch_limit() {
        let now = Instant::now();
        let mut pending = Pending::new(RetentionLimits::new(2, 16));
        assert!(admit(&mut pending, 1, 0, now).is_some());
        assert!(admit(&mut pending, 2, 0, now).is_some());
        pending.accepted(&1, 10);
        pending.accepted(&2, 10);
        (1..=3).for_each(|block| pending.committed(block, block, []));
        assert!(pending.ready(now, 10).0.is_empty());
        assert_eq!(pending.ready(now + GRACE, 1).0, vec![(1, vec![42])]);
        assert_eq!(pending.ready(now + GRACE, 1).0, vec![(2, vec![42])]);
    }
}
