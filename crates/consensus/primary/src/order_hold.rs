//! The own-chain order hold for batch digests (issue #1807).
//!
//! A sender's transactions ride one worker lane in nonce order, so each lane's batch digests must
//! execute in the order the worker sealed them. The proposer drains every lane first in, first
//! out, so two digests of one lane execute out of order only when an older own header dies after
//! a newer own header carried the lane's next digests: the dead header's digests are re-queued
//! and ride a header that executes after the newer one. [`OwnChainOrder`] decides, for the next
//! own header, which lanes it must leave queued so that this cannot happen.
//!
//! # The rule
//!
//! At the build of own header N at round n with parent set P:
//!
//! - N has an own parent when P holds a certificate authored by this node whose header digest
//!   equals the tracked own header at round n-1.
//! - The blockers of N are the tracked own headers below n that carry digests and are not proven
//!   own-chain ancestors of N. With an own parent they are `unproven(n-1)` intersected with the
//!   tracked set; without one, every tracked own header below n that carries digests.
//! - N drains no digest of any worker named in a blocker's payload. Those digests stay queued, in
//!   order.
//! - `unproven(n)` = blockers(N) is recorded for every new header, an empty one included, because
//!   the next header's plan reads it.
//! - A header sent again without a new plan keeps its record (the max-delay re-send). A header that
//!   has none (the restart re-proposal) is recorded as proving no tracked older header.
//!
//! "Tracked" is the proposer's `proposed_headers`: own headers that were built and have been
//! neither committed nor re-queued.
//!
//! The order keeps `unproven(n)` in closed form. An author has one header per round and an own
//! parent is always the header of the round before, so the proven own-chain ancestors of N are
//! the tracked headers from a floor round up to n-1, where floor(N) = floor(n-1) with an own
//! parent and n without one. The blockers of N are then every tracked header below floor(N) that
//! carries digests. A plan reads only the record of round n-1 and the proposer's round only
//! rises, so the order keeps a single record: the newest own header's round and floor. While
//! the proposer adds only its newest header to the tracked set, this gives exactly the sets of
//! the rule; a header that entered the tracked set any other way below the floor blocks, which
//! is the safe side. A tracked own parent without a record proves only itself (floor n-1).
//!
//! # Invariants
//!
//! - I1: a digest of worker w enters N only if every tracked older header carrying w is an
//!   own-chain ancestor of N. [`OwnChainOrder::plan`] enforces it.
//! - I2: no re-queued digest is older, by its worker's sequence, than a digest of the same worker
//!   in a tracked or committed header. The proposer checks it (a counter and a warning in
//!   production, an assertion in tests); this module never sees sequences.
//! - I3: a plan that holds no lane drains as the base does, byte for byte: the held-lane filter
//!   takes no slot and does not move the drain's lane cursor.
//!
//! # Why the hold is enough
//!
//! Consensus orders an older own header before a newer own header, or never. Bullshark sorts a
//! sub-DAG by round, so two own headers ordered in one sub-DAG keep their round order, and once
//! an own header of a higher round commits, the per-author skip (`last_committed`) drops every
//! own header at or below that round for good. An older own header that is not an ancestor of
//! the next own header can still be ordered after that header was built (another author's
//! certificate may cite it), but never after the newer header. So a lane's older digests run
//! after its newer ones only through a re-queue: the cancel re-queue of an older header that was
//! never certified, or the committed-kind re-queue of a certified one, which happens exactly when
//! it died. The proposer cannot know at build time whether an older header will commit first or
//! die, so it holds the lane until a commit notice or a cancel report settles it.
//!
//! - Canceled kind: the certifier cancels own header r-1 only when it receives a newer own header,
//!   and reports it only while no certificate for it is stored, so a canceled header is never an
//!   own parent. While it is tracked it blocks its lanes; its re-queued digests go to the front of
//!   their lanes and ride the next header ahead of anything newer.
//! - Committed kind: by I1, a header N that carries a lane descends through the own chain from
//!   every older tracked header T of that lane. If T later dies, N cannot commit either. The own
//!   header H whose commit skipped T sits above T's round. Up to N's round, the only own header of
//!   each round is on N's own chain, so H would have ordered T (above the GC cut). So H sits above
//!   N's round without N in its history, and its commit skips N for good. T and N are re-queued by
//!   the same commit notice, oldest round first.
//! - Reverse report order: two cancel reports handled before the next header put the newer header's
//!   digests ahead of the older one's (the caveat on the proposer's cancel path). Under the hold
//!   this cannot reorder a lane: two tracked headers that carry the same worker are own-chain
//!   linked, so the older one is certified and never canceled.
//! - The commit notice's lag only keeps a lane held longer.
//!
//! The GC cut is the residual: Bullshark orders a certificate at `leader_round - gc_depth + 1`
//! without visiting its parents, so an own-chain ancestor at or below `leader_round - gc_depth`
//! is not reached through its descendants and can still be overtaken. It is counted (I2), not
//! prevented.
//!
//! # What this module does not decide
//!
//! It does not decide when a header is built, what makes a header valid, or what peers vote for:
//! it only narrows which queued digests this node puts into its own next header. It reads plain
//! values (the round, the parents' authors and header digests, the tracked headers' digests and
//! worker lanes) and has no I/O, clock or logging.
//!
//! The hold still changes when headers are built, through the proposer's trigger. Held digests
//! stay queued, and the proposer's `enough_digests` trigger counts every queued digest, held ones
//! included. A held backlog of at least `header_num_of_batches_threshold` digests therefore
//! builds the next header as soon as its parents allow, without waiting for the min delay, and
//! that header can carry none of the held digests. Under load the trigger fires as on the base,
//! because the queue is above the threshold either way; at light load a held backlog builds a
//! header earlier than the base would. A narrower trigger is not built and is a possible later
//! change: leave held digests out of the count while the only blocker is the own header of the
//! previous round, so that the node waits the base's min delay for that header's certificate.
//!
//! # What releases a held lane
//!
//! A held lane is released when its blockers leave the tracked set: a cancel report re-queues
//! them, a commit notice commits or re-queues them, or the epoch's proposer is dropped with them.
//! Two cases wait for the next own commit:
//!
//! - After a restart, the proposer's highest committed own round (`committed_own_round`) starts at
//!   0, so the max-delay re-send can track the last own header again although it already committed.
//!   The base does the same, and the next own commit at or above that header's round re-queues its
//!   committed digests, which are proposed a second time. The hold only adds the wait: until that
//!   commit, the header holds its lanes.
//! - An own header that the certifier gives up on without sending the uncertified report (no quorum
//!   after every vote request returned, a certificate-store read error, a full report channel)
//!   stays tracked and holds its lanes until the next own commit at or above its round, which
//!   re-queues it. On the base its digests wait just as long, while newer ones overtake them.

use std::collections::{BTreeMap, BTreeSet};
use tn_types::{AuthorityIdentifier, Certificate, Header, HeaderDigest, Round, WorkerId};

/// The rule of the module doc: which worker lanes the next own header must leave queued so that
/// no lane's digests execute out of order.
///
/// It keeps one record, the newest own header's round and the floor of its proven own chain.
/// Call [`Self::plan`] before draining the queue for a new header, [`Self::record`] for every new
/// header (an empty one included), [`Self::record_resent`] for a header sent again without a new
/// plan, and [`Self::forget`] for every round that leaves the tracked set.
///
/// A record names a header by its round alone, so the order must live and die with the epoch's
/// proposer, whose `LastProposed` guard keeps one own header per round. An order carried into
/// another epoch, where rounds start again, could prove ancestry from the wrong header.
#[derive(Debug, Default)]
pub(crate) struct OwnChainOrder {
    /// The newest own header's record, if it has one.
    newest: Option<ChainRecord>,
}

impl OwnChainOrder {
    /// The hold for the own header at `round`, built on `parents`, while `tracked` holds this
    /// node's own headers that were built and neither committed nor re-queued, keyed by round.
    ///
    /// `author` is this node. A parent is the own parent only when `author` wrote it and its
    /// header digest equals the tracked header's at `round - 1`; anything else (a peer's
    /// certificate, an own certificate of another header, a round jump past round `round - 1`)
    /// proves no own-chain ancestry. Tracked headers at or above `round` are ignored.
    ///
    /// `parents` must be exactly the set the header will carry: a plan that sees the own parent
    /// proves ancestry for this header, and its record does so for the next one.
    pub(crate) fn plan<P, H>(
        &self,
        round: Round,
        parents: &[P],
        tracked: &BTreeMap<Round, H>,
        author: &P::Author,
    ) -> HoldPlan
    where
        P: ParentHeader,
        H: TrackedHeader<Digest = P::Digest>,
    {
        let previous = round.checked_sub(1);
        let own_parent = previous.and_then(|prev| tracked.get(&prev)).is_some_and(|own| {
            let digest = own.header_digest();
            parents
                .iter()
                .any(|parent| parent.author() == author && parent.header_digest() == digest)
        });

        let floor = match (previous, self.newest) {
            // the own parent proves itself and, through its record, its own chain below it
            (Some(prev), Some(record)) if own_parent && record.round == prev => record.floor,
            // an own parent without a record proves only itself
            (Some(prev), _) if own_parent => prev,
            // no own parent: no older tracked header is a proven ancestor
            _ => round,
        };

        let mut blockers = BTreeSet::new();
        let mut held_workers = BTreeSet::new();
        for (tracked_round, header) in tracked.range(..floor) {
            let mut carries_digests = false;
            for worker in header.workers() {
                carries_digests = true;
                held_workers.insert(worker);
            }
            // an empty header holds no lane, so it is never a blocker
            if carries_digests {
                blockers.insert(*tracked_round);
            }
        }

        HoldPlan { held_workers, blockers, own_parent, round, floor }
    }

    /// Record the new own header built from `plan`, an empty header included: the next header's
    /// plan reads this record when it cites the header as its own parent.
    ///
    /// The record replaces the previous one, which no later plan reads.
    pub(crate) fn record(&mut self, plan: &HoldPlan) {
        self.newest = Some(ChainRecord { round: plan.round, floor: plan.floor });
    }

    /// Record the own header at `round` sent again without a new plan.
    ///
    /// The max-delay re-send keeps the record it has. A header without one (the restart
    /// re-proposal: a restarted proposer tracks nothing and its ancestry is unknown) is recorded
    /// as proving no tracked older header.
    pub(crate) fn record_resent(&mut self, round: Round) {
        if self.newest.is_some_and(|record| record.round == round) {
            return;
        }
        self.newest = Some(ChainRecord { round, floor: round });
    }

    /// Drop the record of `round`, whose header left the tracked set (committed, or re-queued by
    /// a commit notice or a cancel report).
    ///
    /// A round with no record is a no-op. A plan reads a record only through a tracked own
    /// parent, so the hold's safety does not depend on this call; it keeps the record inside the
    /// tracked set.
    pub(crate) fn forget(&mut self, round: Round) {
        if self.newest.is_some_and(|record| record.round == round) {
            self.newest = None;
        }
    }

    /// The round of the record the order holds, if any.
    #[cfg(test)]
    fn recorded_round(&self) -> Option<Round> {
        self.newest.map(|record| record.round)
    }
}

/// The hold for one own header, from [`OwnChainOrder::plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HoldPlan {
    /// The workers whose digests the header must leave queued: every worker named in a
    /// blocker's payload.
    pub(crate) held_workers: BTreeSet<WorkerId>,
    /// The rounds of the tracked own headers below the header's round that carry digests and are
    /// not proven own-chain ancestors of it.
    pub(crate) blockers: BTreeSet<Round>,
    /// Whether the parents cite this node's tracked header of the previous round.
    pub(crate) own_parent: bool,
    /// The round the plan was made for.
    round: Round,
    /// The lowest round of the header's proven own chain; the round itself without an own parent.
    floor: Round,
}

/// A parent certificate as the hold reads it: who wrote it and its header's digest.
pub(crate) trait ParentHeader {
    /// The identity of an authority.
    type Author: PartialEq;
    /// The digest of a header.
    type Digest: PartialEq;

    /// The authority that wrote the parent's header.
    fn author(&self) -> &Self::Author;

    /// The digest of the parent's header.
    fn header_digest(&self) -> Self::Digest;
}

impl ParentHeader for Certificate {
    type Author = AuthorityIdentifier;
    type Digest = HeaderDigest;

    fn author(&self) -> &AuthorityIdentifier {
        self.origin()
    }

    fn header_digest(&self) -> HeaderDigest {
        self.header().digest()
    }
}

/// A tracked own header as the hold reads it: its digest and the worker of each batch digest in
/// its payload.
pub(crate) trait TrackedHeader {
    /// The digest of a header.
    type Digest: PartialEq;

    /// The header's digest.
    fn header_digest(&self) -> Self::Digest;

    /// The worker of each batch digest in the payload; empty for a header without digests.
    fn workers(&self) -> impl Iterator<Item = WorkerId> + '_;
}

impl TrackedHeader for Header {
    type Digest = HeaderDigest;

    fn header_digest(&self) -> HeaderDigest {
        self.digest()
    }

    fn workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.payload().values().copied()
    }
}

/// The record of one own header: its round and the lowest round of its proven own chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChainRecord {
    /// The header's round.
    round: Round,
    /// The lowest round of the header's proven own chain.
    floor: Round,
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
    use tn_types::{BlockHash, Committee, Hash as _};

    /// This node in the integer model.
    const ME: u8 = 0;

    /// An own header in the integer model: its digest and the worker of each payload digest.
    #[derive(Debug, Clone)]
    struct Own {
        digest: u64,
        workers: Vec<WorkerId>,
    }

    impl TrackedHeader for Own {
        type Digest = u64;

        fn header_digest(&self) -> u64 {
            self.digest
        }

        fn workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
            self.workers.iter().copied()
        }
    }

    /// A parent certificate in the integer model.
    #[derive(Debug, Clone, Copy)]
    struct Parent {
        author: u8,
        digest: u64,
    }

    impl ParentHeader for Parent {
        type Author = u8;
        type Digest = u64;

        fn author(&self) -> &u8 {
            &self.author
        }

        fn header_digest(&self) -> u64 {
            self.digest
        }
    }

    /// The digest of this node's header at `round` in the integer model.
    fn own_digest(round: Round) -> u64 {
        u64::from(round)
    }

    /// The own header at `round` carrying one digest of each worker in `workers`.
    fn own(round: Round, workers: &[WorkerId]) -> Own {
        Own { digest: own_digest(round), workers: workers.to_vec() }
    }

    /// Three peers' certificates of round `round - 1`, plus this node's header of that round when
    /// `with_own`.
    fn parents(round: Round, with_own: bool) -> Vec<Parent> {
        let previous = round.saturating_sub(1);
        let mut parents: Vec<Parent> = (1..=3)
            .map(|author| Parent {
                author,
                digest: 1_000 * u64::from(author) + u64::from(previous),
            })
            .collect();
        if with_own {
            parents.push(Parent { author: ME, digest: own_digest(previous) });
        }
        parents
    }

    /// The set of `items`.
    fn set<T: Ord + Copy>(items: &[T]) -> BTreeSet<T> {
        items.iter().copied().collect()
    }

    /// The proposer side of the integer model: the order and the tracked own headers.
    #[derive(Debug, Default)]
    struct Model {
        order: OwnChainOrder,
        tracked: BTreeMap<Round, Own>,
    }

    impl Model {
        /// Build and record the own header at `round` while each worker in `queued` has a digest
        /// waiting, citing this node's previous header when `with_own`. The header carries one
        /// digest of every queued worker the plan does not hold.
        fn build(
            &mut self,
            round: Round,
            with_own: bool,
            queued: &[WorkerId],
        ) -> (HoldPlan, Vec<WorkerId>) {
            let plan = self.order.plan(round, &parents(round, with_own), &self.tracked, &ME);
            let carried: Vec<WorkerId> = queued
                .iter()
                .copied()
                .filter(|worker| !plan.held_workers.contains(worker))
                .collect();
            self.order.record(&plan);
            self.tracked.insert(round, own(round, &carried));
            (plan, carried)
        }

        /// The header at `round` leaves the tracked set: committed, or re-queued by a commit
        /// notice or a cancel report.
        fn remove(&mut self, round: Round) {
            self.tracked.remove(&round);
            self.order.forget(round);
        }
    }

    #[test]
    fn intact_own_chain_holds_nothing() {
        let mut model = Model::default();
        let (first, carried) = model.build(1, false, &[0, 1]);
        assert!(!first.own_parent);
        assert!(first.blockers.is_empty());
        assert_eq!(carried, vec![0, 1]);

        for round in 2..=12 {
            let (plan, carried) = model.build(round, true, &[0, 1]);
            assert!(plan.own_parent, "round {round}");
            assert!(plan.blockers.is_empty(), "round {round}: {plan:?}");
            assert!(plan.held_workers.is_empty(), "round {round}: {plan:?}");
            assert_eq!(carried, vec![0, 1], "round {round}");
        }
    }

    #[test]
    fn missing_own_parent_blocks_every_older_header_with_digests() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        model.build(2, true, &[1]);
        model.build(3, true, &[]);

        let (plan, carried) = model.build(4, false, &[0, 1, 2]);
        assert!(!plan.own_parent);
        // the empty header at round 3 names no worker, so it blocks nothing
        assert_eq!(plan.blockers, set(&[1, 2]));
        assert_eq!(plan.held_workers, set(&[0, 1]));
        assert_eq!(carried, vec![2]);
    }

    #[test]
    fn unproven_carries_through_the_own_chain_until_its_header_leaves() {
        let mut model = Model::default();
        model.build(1, false, &[0]);

        // round 1's certificate misses round 2's parents (the race): lane 0 is held, and the
        // header goes out empty
        let (plan, carried) = model.build(2, false, &[0]);
        assert!(!plan.own_parent);
        assert_eq!(plan.blockers, set(&[1]));
        assert!(carried.is_empty());

        // an intact own chain on top of round 2 proves nothing about round 1
        for round in 3..=7 {
            let (plan, carried) = model.build(round, true, &[0, 1]);
            assert!(plan.own_parent, "round {round}");
            assert_eq!(plan.blockers, set(&[1]), "round {round}");
            assert_eq!(plan.held_workers, set(&[0]), "round {round}");
            assert_eq!(carried, vec![1], "round {round}");
        }

        // round 1 commits, or a commit notice re-queues its digests at the front of lane 0
        model.remove(1);
        let (plan, carried) = model.build(8, true, &[0, 1]);
        assert!(plan.blockers.is_empty());
        assert_eq!(carried, vec![0, 1]);
    }

    #[test]
    fn empty_header_still_records() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        // nothing queued: the header at round 2 is empty, on an intact own chain
        let (plan, carried) = model.build(2, true, &[]);
        assert!(plan.blockers.is_empty());
        assert!(carried.is_empty());
        assert_eq!(model.order.recorded_round(), Some(2));

        // the empty header's record carries the proof that round 1 is an ancestor
        let (plan, carried) = model.build(3, true, &[0]);
        assert!(plan.blockers.is_empty());
        assert_eq!(carried, vec![0]);

        // without that record, round 3 could prove nothing below its own parent and would hold
        // lane 0 behind round 1
        let mut unrecorded = Model::default();
        unrecorded.build(1, false, &[0]);
        unrecorded.tracked.insert(2, own(2, &[]));
        let (plan, carried) = unrecorded.build(3, true, &[0]);
        assert!(plan.own_parent);
        assert_eq!(plan.blockers, set(&[1]));
        assert!(carried.is_empty());
    }

    #[test]
    fn restart_reproposal_records_the_tracked_older_headers() {
        // a restarted proposer tracks nothing and re-sends its stored header of round 7
        let mut model = Model::default();
        model.tracked.insert(7, own(7, &[0]));
        model.order.record_resent(7);
        assert_eq!(model.order.recorded_round(), Some(7));
        let (plan, carried) = model.build(8, true, &[0]);
        assert!(plan.own_parent);
        assert!(plan.blockers.is_empty());
        assert_eq!(carried, vec![0]);

        // older tracked headers would not be proven by the re-proposal
        let mut model = Model::default();
        model.tracked.insert(5, own(5, &[1]));
        model.tracked.insert(6, own(6, &[]));
        model.tracked.insert(7, own(7, &[0]));
        model.order.record_resent(7);
        let (plan, carried) = model.build(8, true, &[0, 1]);
        assert!(plan.own_parent);
        assert_eq!(plan.blockers, set(&[5]));
        assert_eq!(plan.held_workers, set(&[1]));
        assert_eq!(carried, vec![0]);
    }

    #[test]
    fn max_delay_resend_keeps_the_record() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        // round 2 misses its own parent: lane 0 is held behind round 1, lane 1 rides round 2
        model.build(2, false, &[0, 1]);
        model.build(3, true, &[1]);

        // max delay fires at round 3 with no new parents: the stored header goes out again
        model.order.record_resent(3);
        assert_eq!(model.order.recorded_round(), Some(3));

        // the record still proves round 2 (a fresh record would block rounds 1 and 2)
        let (plan, carried) = model.build(4, true, &[0, 1]);
        assert!(plan.own_parent);
        assert_eq!(plan.blockers, set(&[1]));
        assert_eq!(plan.held_workers, set(&[0]));
        assert_eq!(carried, vec![1]);
    }

    #[test]
    fn resend_after_the_record_was_forgotten_proves_nothing_older() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        model.build(2, true, &[0]);
        // round 2 leaves the tracked set, then the max-delay re-send tracks it again
        model.remove(2);
        assert_eq!(model.order.recorded_round(), None);
        model.tracked.insert(2, own(2, &[0]));
        model.order.record_resent(2);

        let (plan, carried) = model.build(3, true, &[0]);
        assert!(plan.own_parent);
        assert_eq!(plan.blockers, set(&[1]));
        assert!(carried.is_empty());
    }

    #[test]
    fn only_the_named_lane_is_held() {
        let mut model = Model::default();
        model.build(1, false, &[0]);

        let (plan, carried) = model.build(2, false, &[0, 1]);
        assert_eq!(plan.blockers, set(&[1]));
        assert_eq!(plan.held_workers, set(&[0]));
        assert_eq!(carried, vec![1]);

        // round 2 carries lane 1 and is the own parent, so lane 1 stays free
        let (plan, carried) = model.build(3, true, &[0, 1]);
        assert_eq!(plan.blockers, set(&[1]));
        assert_eq!(plan.held_workers, set(&[0]));
        assert_eq!(carried, vec![1]);
    }

    #[test]
    fn round_jump_blocks_every_older_header_with_digests() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        model.build(2, true, &[1]);

        // the round jumps from 2 to 5: the parents are round 4's, where this node built nothing
        let (plan, carried) = model.build(5, true, &[0, 1, 2]);
        assert!(!plan.own_parent);
        assert_eq!(plan.blockers, set(&[1, 2]));
        assert_eq!(plan.held_workers, set(&[0, 1]));
        assert_eq!(carried, vec![2]);
    }

    #[test]
    fn only_this_nodes_tracked_header_is_an_own_parent() {
        let mut model = Model::default();
        model.build(1, false, &[0]);
        model.build(2, true, &[1]);

        // a peer's certificate with the tracked header's digest value
        let peer_copy = vec![Parent { author: 1, digest: own_digest(2) }];
        // this node's certificate of a header other than the tracked one
        let other_header = vec![Parent { author: ME, digest: 99 }];
        for parents in [peer_copy, other_header] {
            let plan = model.order.plan(3, &parents, &model.tracked, &ME);
            assert!(!plan.own_parent, "{parents:?}");
            assert_eq!(plan.blockers, set(&[1, 2]), "{parents:?}");
        }

        // a tracked header at or above the round being built is never a blocker
        model.tracked.insert(3, own(3, &[2]));
        let plan = model.order.plan(3, &parents(3, false), &model.tracked, &ME);
        assert_eq!(plan.blockers, set(&[1, 2]));
        assert_eq!(plan.held_workers, set(&[0, 1]));

        // round 0 has no previous round to cite
        let plan = OwnChainOrder::default().plan(0, &parents(0, true), &model.tracked, &ME);
        assert!(!plan.own_parent);
        assert!(plan.blockers.is_empty());
    }

    #[test]
    fn forget_of_a_round_never_recorded_changes_nothing() {
        let mut empty = OwnChainOrder::default();
        empty.forget(4);
        assert_eq!(empty.recorded_round(), None);

        let mut model = Model::default();
        model.build(1, false, &[0]);
        model.build(2, false, &[0, 1]);
        let before = model.order.plan(3, &parents(3, true), &model.tracked, &ME);

        // round 1 is tracked but not the newest record; rounds 0 and 9 were never built
        for round in [1, 0, 9] {
            model.order.forget(round);
            assert_eq!(model.order.recorded_round(), Some(2), "forget({round})");
        }
        assert_eq!(model.order.plan(3, &parents(3, true), &model.tracked, &ME), before);
    }

    #[test]
    fn the_record_stays_inside_the_tracked_set() {
        let mut model = Model::default();
        for round in 1..=300 {
            // every seventh header misses its own parent
            model.build(round, round % 7 != 0, &[0, 1]);
            // a commit notice every fifth round settles everything two rounds back
            if round % 5 == 0 {
                let settled: Vec<Round> =
                    model.tracked.range(..round.saturating_sub(1)).map(|(r, _)| *r).collect();
                settled.into_iter().for_each(|r| model.remove(r));
            }
            // a cancel report of the newest header every eleventh round
            if round % 11 == 0 {
                model.remove(round);
            }
            if let Some(recorded) = model.order.recorded_round() {
                assert!(model.tracked.contains_key(&recorded), "round {round}");
            }
        }
    }

    /// The design's rule written literally: `unproven(n)` kept per round and read as
    /// `unproven(n - 1)` intersected with the tracked set. Under an own parent with no
    /// `unproven(n - 1)` it counts every tracked header below n - 1 with digests, as the order
    /// does.
    #[derive(Debug, Default)]
    struct DesignRule {
        unproven: BTreeMap<Round, BTreeSet<Round>>,
    }

    impl DesignRule {
        /// Every tracked header below `round` that carries digests.
        fn older_with_digests(tracked: &BTreeMap<Round, Own>, round: Round) -> BTreeSet<Round> {
            tracked
                .range(..round)
                .filter(|(_, header)| !header.workers.is_empty())
                .map(|(tracked_round, _)| *tracked_round)
                .collect()
        }

        /// The blockers of the header at `round`.
        fn blockers(
            &self,
            round: Round,
            own_parent: bool,
            tracked: &BTreeMap<Round, Own>,
        ) -> BTreeSet<Round> {
            if !own_parent {
                return Self::older_with_digests(tracked, round);
            }
            match self.unproven.get(&(round - 1)) {
                Some(unproven) => unproven
                    .iter()
                    .copied()
                    .filter(|blocker| tracked.contains_key(blocker))
                    .collect(),
                None => Self::older_with_digests(tracked, round - 1),
            }
        }

        /// A re-send keeps its record; one without a record records the tracked older headers.
        fn record_resent(&mut self, round: Round, tracked: &BTreeMap<Round, Own>) {
            self.unproven.entry(round).or_insert_with(|| Self::older_with_digests(tracked, round));
        }
    }

    /// Every tracked older header with digests that a plan leaves out of its blockers is an
    /// own-chain ancestor of the header being built (I1), over seeded random walks of builds,
    /// skipped rounds, re-sends, unrecorded builds and removals. The ancestry is walked
    /// link by link here, independently of the order's floor.
    ///
    /// The blockers also equal the design's literal rule while the tracked set only gains the
    /// newest header. Odd walks also put older headers back into the tracked set (no path on the
    /// proposer does this); there the blockers must hold at least the literal rule's.
    #[test]
    fn plan_releases_a_lane_only_to_a_descendant_of_its_older_headers() {
        // xorshift64: a fixed seed keeps the walks reproducible
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for walk in 0..3_000 {
            let mut model = Model::default();
            let mut design = DesignRule::default();
            // false once an older header has re-entered the tracked set
            let mut only_newest_enters = true;
            // every header built: its payload's workers and its own parent's round, if proven
            let mut built: BTreeMap<Round, (Vec<WorkerId>, Option<Round>)> = BTreeMap::new();
            let mut round: Round = 0;

            for step in 0..32 {
                match next() % 12 {
                    // the round advances without an own header (a round jump)
                    0 => round += 1 + Round::try_from(next() % 2).unwrap_or(0),
                    // a cancel report or a commit removes one tracked header
                    1 => {
                        let keys: Vec<Round> = model.tracked.keys().copied().collect();
                        if !keys.is_empty() {
                            let index = usize::try_from(next()).unwrap_or(0) % keys.len();
                            model.remove(keys[index]);
                            design.unproven.remove(&keys[index]);
                        }
                    }
                    // a commit notice settles every tracked header up to one of them
                    2 => {
                        let keys: Vec<Round> = model.tracked.keys().copied().collect();
                        if !keys.is_empty() {
                            let index = usize::try_from(next()).unwrap_or(0) % keys.len();
                            for settled in &keys[..=index] {
                                model.remove(*settled);
                                design.unproven.remove(settled);
                            }
                        }
                    }
                    // the max-delay re-send of the newest header, tracked again if it had left
                    3 => {
                        if let Some((workers, _)) = built.get(&round) {
                            design.record_resent(round, &model.tracked);
                            model.tracked.insert(round, own(round, workers));
                            model.order.record_resent(round);
                        }
                    }
                    // an older header that left the tracked set comes back into it
                    4 if walk % 2 == 1 => {
                        let gone: Vec<Round> = built
                            .range(..round)
                            .map(|(built_round, _)| *built_round)
                            .filter(|built_round| !model.tracked.contains_key(built_round))
                            .collect();
                        if !gone.is_empty() {
                            let back = gone[usize::try_from(next()).unwrap_or(0) % gone.len()];
                            let workers =
                                built.get(&back).map(|(w, _)| w.clone()).unwrap_or_default();
                            model.tracked.insert(back, own(back, &workers));
                            only_newest_enters = false;
                        }
                    }
                    choice => {
                        round += 1;
                        let with_own = next() % 3 != 0;
                        let queued: Vec<WorkerId> = (0..3).filter(|_| next() % 2 == 0).collect();
                        let plan =
                            model.order.plan(round, &parents(round, with_own), &model.tracked, &ME);

                        let literal = design.blockers(round, plan.own_parent, &model.tracked);
                        if only_newest_enters {
                            assert_eq!(plan.blockers, literal, "walk {walk} step {step}");
                        } else {
                            assert!(plan.blockers.is_superset(&literal), "walk {walk} step {step}");
                        }

                        // the own parent is proven only through the tracked header of the round
                        // before
                        let linked =
                            with_own && model.tracked.contains_key(&round.saturating_sub(1));
                        assert_eq!(plan.own_parent, linked, "walk {walk} step {step}");
                        let mut ancestors = BTreeSet::new();
                        let mut cursor = linked.then_some(round - 1);
                        while let Some(ancestor) = cursor {
                            ancestors.insert(ancestor);
                            cursor = built.get(&ancestor).and_then(|(_, parent)| *parent);
                        }

                        for (tracked_round, header) in model.tracked.range(..round) {
                            if header.workers.is_empty() {
                                assert!(!plan.blockers.contains(tracked_round));
                                continue;
                            }
                            if plan.blockers.contains(tracked_round) {
                                assert!(header
                                    .workers
                                    .iter()
                                    .all(|worker| plan.held_workers.contains(worker)));
                            } else {
                                assert!(
                                    ancestors.contains(tracked_round),
                                    "walk {walk} step {step}: round {tracked_round} released \
                                     to round {round} without ancestry: {plan:?}"
                                );
                            }
                        }

                        let carried: Vec<WorkerId> = queued
                            .into_iter()
                            .filter(|worker| !plan.held_workers.contains(worker))
                            .collect();
                        // one build in seven is never recorded, to reach the missing-record path
                        if choice != 11 {
                            model.order.record(&plan);
                            design.unproven.insert(round, literal);
                        }
                        model.tracked.insert(round, own(round, &carried));
                        built.insert(round, (carried, linked.then_some(round - 1)));
                    }
                }
                if let Some(recorded) = model.order.recorded_round() {
                    assert!(model.tracked.contains_key(&recorded), "walk {walk} step {step}");
                }
            }
        }
    }

    /// A payload with one batch digest, filled with the given byte, per (byte, worker) entry.
    fn payload(entries: &[(u8, WorkerId)]) -> IndexMap<BlockHash, WorkerId> {
        entries.iter().map(|(byte, worker)| (BlockHash::repeat_byte(*byte), *worker)).collect()
    }

    /// The header at `round` written by `author` on genesis parents, with `entries` as payload.
    fn header_at(
        author: &AuthorityFixture<MemDatabase>,
        committee: &Committee,
        round: Round,
        entries: &[(u8, WorkerId)],
    ) -> Header {
        author.header_builder_at_round(committee, round).payload(payload(entries)).build()
    }

    #[test]
    fn plans_from_the_proposers_own_types() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let me = fixture.authorities().next().expect("the fixture has authorities");

        let mut tracked: BTreeMap<Round, Header> = BTreeMap::new();
        tracked.insert(1, header_at(me, &committee, 1, &[(1, 0)]));
        let first_certificate = fixture.certificate(&tracked[&1]);
        // round 2 cites round 1's certificate, as the replay below assumes
        let second = me
            .header_builder_at_round(&committee, 2)
            .payload(payload(&[(2, 1), (3, 1)]))
            .parents(BTreeSet::from([first_certificate.digest()]))
            .build();
        tracked.insert(2, second);

        let peers: Vec<Certificate> = fixture
            .authorities()
            .skip(1)
            .map(|peer| fixture.certificate(&header_at(peer, &committee, 2, &[])))
            .collect();
        let mut with_own = peers.clone();
        with_own.push(fixture.certificate(&tracked[&2]));

        let mut order = OwnChainOrder::default();
        // the own parent without a record proves only itself
        let plan = order.plan(3, &with_own, &tracked, &me.id());
        assert!(plan.own_parent);
        assert_eq!(plan.blockers, set(&[1]));
        assert_eq!(plan.held_workers, set(&[0]));

        // replay the chain: round 1 on genesis, round 2 citing round 1's certificate
        let genesis: Vec<Certificate> = Vec::new();
        let first = order.plan(1, &genesis, &BTreeMap::<Round, Header>::new(), &me.id());
        order.record(&first);
        let below_two: BTreeMap<Round, Header> =
            tracked.range(..2).map(|(round, header)| (*round, header.clone())).collect();
        let second = order.plan(2, &[first_certificate], &below_two, &me.id());
        assert!(second.own_parent);
        order.record(&second);
        // with round 2's record proving round 1, nothing is held
        let plan = order.plan(3, &with_own, &tracked, &me.id());
        assert!(plan.own_parent);
        assert!(plan.blockers.is_empty());
        assert!(plan.held_workers.is_empty());

        // without the own certificate in the parents every tracked header with digests blocks
        let plan = order.plan(3, &peers, &tracked, &me.id());
        assert!(!plan.own_parent);
        assert_eq!(plan.blockers, set(&[1, 2]));
        assert_eq!(plan.held_workers, set(&[0, 1]));
    }
}

#[cfg(test)]
#[path = "tests/order_hold_model.rs"]
mod model;
