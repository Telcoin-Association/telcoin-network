//! Model test of the own-chain order hold (issue #1807) against one author's proposer.
//!
//! The unit tests in `order_hold.rs` check the rule's arithmetic. This suite checks the rule
//! against the system it lives in: one author's proposer under random certification, cancels,
//! commits, re-sends and restarts. It drives the real [`OwnChainOrder`] through its two traits
//! with integer types and models everything else as main (45dce8947) does it:
//!
//! - `Report`: a worker reports sealed batches; each digest goes to the back of its lane with the
//!   lane's next sequence number (`proposer.rs:1230`).
//! - `Build`: the next own header. The header still in vote collection gets its certificate first
//!   or is canceled when the newer header reaches the certifier (`certifier.rs:812-838`), and the
//!   cancel report travels through a channel that can drop it (`certifier.rs:781-801`). The round
//!   advances by one or jumps (`proposer.rs:1013-1017`). The parents are three peers plus, when
//!   wanted and certified, this node's certificate of the previous round. With the hold on, the
//!   model takes the plan from those parents, drains only the lanes the plan does not hold and
//!   records the plan; the drain is main's: lanes round-robin from the cursor, first in first out
//!   within a lane, up to the header cap (`proposer.rs:974-999`). The header is tracked
//!   (`proposer.rs:1133`).
//! - `Certify`: the header in vote collection gets its certificate.
//! - `DeliverCancel`: `process_uncertified_header` (`proposer.rs:902-921`): only the exact header
//!   tracked at that round and below the proposer's round; its digests go to the front of their
//!   lanes, oldest first (`push_payload` and `prepend_requeued`, `proposer.rs:1386-1399`,
//!   `:950-954`).
//! - `Commit`: consensus commits a sub-DAG holding own headers (`consensus/utils.rs:31-111`): a
//!   certified own header above the author's last committed round, its own-chain ancestors reached
//!   through own-parent links (no expansion past a certificate at `gc_round + 1`, a parent at or
//!   below `last_committed` skipped), plus older certified own headers another author's certificate
//!   may reach. The sub-DAG is sorted by round and each header's digests execute in payload order
//!   (DR-1807 Q6), so an older own header is ordered before a newer one or never. The commit notice
//!   is queued, or merged into the pending one as one `process_certificate` call does for several
//!   sub-DAGs (`consensus/state.rs:746-814`).
//! - `DeliverNotice`: `process_committed_headers` (`proposer.rs:823-876`): the committed rounds
//!   leave the tracked set, then every tracked header at or below the highest committed round is
//!   re-queued in one call, oldest round first, to the front of the lanes.
//! - `Resend`: the max-delay re-send of the newest header while it is still tracked at the
//!   proposer's round (`proposer.rs:1171-1185`, `:1335-1356`, inserted again at `:1133`). A re-send
//!   of a header whose commit notice was already handled is left out: it puts committed digests
//!   back into the tracked set on main whether or not the hold is on.
//! - `Restart`: the node restarts inside the epoch. Reports and notices in flight are lost, the
//!   queue and the tracked headers go to the orphan re-pool, and a fresh proposer (empty queue,
//!   tracked set, order and cursor) re-proposes the newest header when it is above the last
//!   committed round (`proposer.rs:1036-1070`; main's own condition, the newest header above the
//!   primary round, is stricter). A committed newest header is never re-proposed: main can re-send
//!   it after a restart (the first max-delay tick, before any parents arrive), which is the
//!   excluded re-send above.
//!
//! The properties, with the hold on:
//! - P1: each lane's executed sequence numbers strictly increase;
//! - P2: no digest executes twice;
//! - P3: every reported digest is exactly one of executed, queued, in a tracked header that has not
//!   committed, or orphaned by a restart;
//! - P4: from any state, healthy rounds whose commits lag the build by `lag` headers hold no lane
//!   after the first `lag + 1` and drain the queue within [`Model::liveness_bound`] rounds;
//! - P5: with the own chain intact the hold never holds anything and the headers equal the hold-off
//!   headers.
//!
//! With the hold off the same search must find a P1 violation, or the model is too weak to see
//! the defect. With a small GC depth every P1 violation must be the GC-cut residual: an own-chain
//! ancestor cut by the GC depth below the leader, whose lane a descendant took while it was
//! tracked. The newer digest then runs first from that descendant, or its re-queue lands ahead
//! of the cut header's when the descendant dies too and the two re-queues are separate calls.

use super::{OwnChainOrder, ParentHeader, TrackedHeader};
use proptest::{
    prelude::*,
    strategy::{Union, ValueTree},
    test_runner::{RngAlgorithm, TestError, TestRng, TestRunner},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};
use tn_types::{Round, WorkerId};

/// This node in the model.
const ME: u8 = 0;

/// A GC depth no schedule of the model reaches: the traversal never cuts.
const NO_GC_CUT: Round = Round::MAX;

/// The longest schedule the strategies generate.
const MAX_STEPS: usize = 80;

/// Cases of each default property run (a case takes about a millisecond).
const CASES: u32 = 2_048;

/// Cases the hold-off search may use to find an overtaken lane.
const SANITY_CASES: u32 = 4_096;

/// The fixed seed of the hold-off search.
const SANITY_SEED: [u8; 32] = [0x18; 32];

/// The fewest of `CASES` default schedules that must overtake a lane with the hold off.
const MIN_HOLD_OFF_HITS: usize = 64;

/// The index of a reported digest in [`Model::digests`].
type DigestId = usize;

/// The digest of the own header at `round`: an author has one header per round.
fn own_header_digest(round: Round) -> u64 {
    u64::from(round)
}

/// The model of one author: the consensus side that survives a restart, the channels into the
/// proposer, the proposer itself, and what the checks observed.
#[derive(Debug)]
struct Model {
    /// The schedule's fixed parameters.
    params: Params,
    /// Whether the proposer consults the order.
    hold: bool,
    /// The step counter; every action takes its own step.
    step: usize,
    /// Every reported digest, by id.
    digests: Vec<DigestInfo>,
    /// The next sequence number of each lane.
    next_seq: BTreeMap<WorkerId, u64>,
    /// Every own header ever built, by round.
    dag: BTreeMap<Round, DagHeader>,
    /// The highest committed own round (`last_committed`), 0 before the first commit.
    last_committed: Round,
    /// The own header in vote collection at the certifier.
    in_vote: Option<Round>,
    /// The newest own header (`LastProposed`).
    newest: Option<Round>,
    /// Cancel reports on their way to the proposer, oldest first.
    cancel_reports: VecDeque<(Round, u64)>,
    /// Commit notices on their way to the proposer, oldest first: leader round, own rounds.
    notices: VecDeque<(Round, Vec<Round>)>,
    /// The proposer, rebuilt on a restart.
    proposer: ProposerModel,
    /// Executed digests in execution order.
    executed: Vec<DigestId>,
    /// Executed digests per lane, in execution order.
    executed_by_lane: BTreeMap<WorkerId, Vec<DigestId>>,
    /// Digests a restart handed to the orphan re-pool that have not executed since.
    orphaned: BTreeSet<DigestId>,
    /// Every header built, in build order.
    built: Vec<Built>,
    /// P1 violations.
    overtakes: Vec<Overtake>,
    /// P2 violations: digests executed a second time.
    duplicates: Vec<DigestId>,
    /// The first P3 violation.
    accounting: Option<String>,
}

impl Model {
    /// A model before any step.
    fn new(params: Params, hold: bool) -> Self {
        Self {
            params,
            hold,
            step: 0,
            digests: Vec::new(),
            next_seq: BTreeMap::new(),
            dag: BTreeMap::new(),
            last_committed: 0,
            in_vote: None,
            newest: None,
            cancel_reports: VecDeque::new(),
            notices: VecDeque::new(),
            proposer: ProposerModel::default(),
            executed: Vec::new(),
            executed_by_lane: BTreeMap::new(),
            orphaned: BTreeSet::new(),
            built: Vec::new(),
            overtakes: Vec::new(),
            duplicates: Vec::new(),
            accounting: None,
        }
    }

    /// Run every step of `case`, with or without the hold.
    fn run(case: &Case, hold: bool) -> Self {
        let mut model = Self::new(case.params, hold);
        for step in &case.steps {
            model.act(step);
        }
        model
    }

    /// Apply one step, then check P3.
    fn act(&mut self, step: &Step) {
        match *step {
            Step::Report { worker, count } => self.report(worker, count),
            Step::Build { certify_prev, own_parent, jump, drop_report } => {
                self.build(certify_prev, own_parent, jump, drop_report)
            }
            Step::Certify => self.certify(),
            Step::DeliverCancel => self.deliver_cancel(),
            Step::Commit { top, picks, lead, merge } => self.commit_step(top, picks, lead, merge),
            Step::DeliverNotice => self.deliver_notice(),
            Step::Resend => self.resend(),
            Step::Restart { repropose } => self.restart(repropose),
        }
        self.check_accounting();
    }

    /// A worker reports `count` sealed batches: each digest goes to the back of its lane with the
    /// lane's next sequence number.
    fn report(&mut self, worker: WorkerId, count: u8) {
        self.step += 1;
        let worker = worker % self.params.workers;
        for _ in 0..count {
            let id = self.digests.len();
            let next = self.next_seq.entry(worker).or_default();
            let seq = *next;
            *next += 1;
            self.digests.push(DigestInfo {
                worker,
                seq,
                stays: Vec::new(),
                requeued: false,
                executed_in: None,
            });
            self.proposer.queue.entry(worker).or_default().push_back(id);
        }
    }

    /// Build the next own header.
    ///
    /// The header in vote collection is certified first when `certify_prev`, otherwise the new
    /// header cancels it and the cancel report is queued unless `drop_report`. The parents cite
    /// this node's previous header when `own_parent` and it is certified.
    fn build(&mut self, certify_prev: bool, own_parent: bool, jump: u8, drop_report: bool) {
        self.step += 1;
        if let Some(previous) = self.in_vote.take() {
            if certify_prev {
                self.dag_mut(previous).certified = true;
            } else if !drop_report {
                self.cancel_reports.push_back((previous, own_header_digest(previous)));
            }
        }

        let round = self.proposer.round + 1 + Round::from(jump);
        let previous = round - 1;
        let own_parent = own_parent && self.dag.get(&previous).is_some_and(|h| h.certified);
        let mut parents: Vec<Parent> = (1..=3)
            .map(|author| Parent {
                author,
                digest: (u64::from(author) << 40) | u64::from(previous),
            })
            .collect();
        if own_parent {
            parents.push(Parent { author: ME, digest: own_header_digest(previous) });
        }

        let plan = self
            .hold
            .then(|| self.proposer.order.plan(round, &parents, &self.proposer.tracked, &ME));
        let held = plan.as_ref().map(|plan| plan.held_workers.clone()).unwrap_or_default();
        let payload = self.proposer.drain(self.params.cap, &held);
        if let Some(plan) = &plan {
            self.proposer.order.record(plan);
        }

        for digest in &payload {
            self.digests[*digest].stays.push(Stay { header: round, from: self.step, until: None });
        }
        let lanes = payload.iter().map(|digest| (*digest, self.digests[*digest].worker)).collect();
        self.proposer.tracked.insert(round, Tracked { round, payload: lanes });
        self.dag.insert(
            round,
            DagHeader {
                payload: payload.clone(),
                own_parent,
                certified: false,
                committed: false,
                built_step: self.step,
                death: None,
            },
        );
        self.built.push(Built { round, payload, held });
        self.in_vote = Some(round);
        self.newest = Some(round);
        self.proposer.round = round;
    }

    /// The header in vote collection gets its certificate.
    fn certify(&mut self) {
        self.step += 1;
        if let Some(round) = self.in_vote.take() {
            self.dag_mut(round).certified = true;
        }
    }

    /// The proposer handles the oldest cancel report (`process_uncertified_header`).
    fn deliver_cancel(&mut self) {
        self.step += 1;
        let Some((round, digest)) = self.cancel_reports.pop_front() else {
            return;
        };
        let acts =
            self.proposer.tracked.get(&round).is_some_and(|header| {
                round < self.proposer.round && header.header_digest() == digest
            });
        if acts {
            self.requeue(&[round]);
        }
    }

    /// Re-queue the tracked headers at `rounds` (ascending) in one call: their digests go to the
    /// front of their lanes, oldest round first and payload order within a header, and the
    /// headers leave the tracked set.
    fn requeue(&mut self, rounds: &[Round]) {
        let mut lanes: BTreeMap<WorkerId, VecDeque<DigestId>> = BTreeMap::new();
        for round in rounds {
            let Some(header) = self.proposer.tracked.remove(round) else {
                continue;
            };
            for (digest, worker) in header.payload {
                lanes.entry(worker).or_default().push_back(digest);
                let info = &mut self.digests[digest];
                info.requeued = true;
                if let Some(stay) = info.stays.iter_mut().rev().find(|stay| stay.header == *round) {
                    if stay.until.is_none() {
                        stay.until = Some(self.step);
                    }
                }
            }
            if self.hold {
                self.proposer.order.forget(*round);
            }
        }
        for (worker, mut pending) in std::mem::take(&mut self.proposer.queue) {
            lanes.entry(worker).or_default().append(&mut pending);
        }
        self.proposer.queue = lanes;
    }

    /// A commit chosen by selectors: `top` picks the newest own header of the commit among the
    /// certified ones above the last committed round, `picks` the older ones another author's
    /// certificate reaches, `lead` the leader's distance above `top` (below the GC depth).
    fn commit_step(&mut self, top: u8, picks: u16, lead: u8, merge: bool) {
        let candidates: Vec<Round> = self
            .dag
            .iter()
            .filter(|(round, h)| h.certified && !h.committed && **round > self.last_committed)
            .map(|(round, _)| *round)
            .collect();
        if candidates.is_empty() {
            self.step += 1;
            return;
        }
        let top = candidates[usize::from(top) % candidates.len()];
        let lead = Round::from(lead) % self.params.gc_depth;
        let gc_round = (top + lead).saturating_sub(self.params.gc_depth);
        let picked: Vec<Round> = candidates
            .iter()
            .copied()
            .filter(|round| *round < top && *round > gc_round)
            .enumerate()
            .filter(|(index, _)| *index < 16 && picks & (1 << index) != 0)
            .map(|(_, round)| round)
            .collect();
        self.commit_at(top, &picked, lead, merge);
    }

    /// Commit `top` and `picked` with the leader `lead` rounds above `top`: each with its own-chain
    /// ancestors above the GC cut and above the last committed round, executed in round order.
    fn commit_at(&mut self, top: Round, picked: &[Round], lead: Round, merge: bool) {
        self.step += 1;
        let leader = top + lead;
        let gc_round = leader.saturating_sub(self.params.gc_depth);
        let mut included = BTreeSet::new();
        let mut stack: Vec<Round> = std::iter::once(top).chain(picked.iter().copied()).collect();
        while let Some(round) = stack.pop() {
            if !included.insert(round) {
                continue;
            }
            // a certificate at gc_round + 1 is ordered but its parents are not visited
            if round == gc_round + 1 {
                continue;
            }
            // a parent at or below the author's last committed round is skipped
            if self.dag[&round].own_parent && round - 1 > self.last_committed {
                stack.push(round - 1);
            }
        }
        self.last_committed = self.last_committed.max(top);

        // every header below the new last committed round that never committed is dead for good
        let dead: Vec<Round> = self
            .dag
            .range(..self.last_committed)
            .filter(|(round, h)| !h.committed && h.death.is_none() && !included.contains(*round))
            .map(|(round, _)| *round)
            .collect();
        for round in dead {
            let cut = included.iter().any(|header| self.descends(*header, round));
            self.dag_mut(round).death = Some(if cut { Death::GcCut } else { Death::Skipped });
        }

        for round in &included {
            self.dag_mut(*round).committed = true;
            let payload = self.dag[round].payload.clone();
            for digest in payload {
                self.execute(digest, *round);
            }
        }

        let rounds: Vec<Round> = included.into_iter().collect();
        match self.notices.back_mut() {
            Some(pending) if merge => {
                pending.0 = pending.0.max(leader);
                pending.1.extend(rounds);
            }
            _ => self.notices.push_back((leader, rounds)),
        }
    }

    /// Execute `digest` from the committed header at `round`, checking P1 and P2.
    fn execute(&mut self, digest: DigestId, round: Round) {
        if self.digests[digest].executed_in.is_some() {
            self.duplicates.push(digest);
        }
        let worker = self.digests[digest].worker;
        let seq = self.digests[digest].seq;
        let overtaken_by: Vec<DigestId> = self
            .executed_by_lane
            .get(&worker)
            .into_iter()
            .flatten()
            .copied()
            .filter(|early| self.digests[*early].seq > seq)
            .collect();
        for early in overtaken_by {
            let residual = self.residual(digest, early);
            self.overtakes.push(Overtake {
                late: digest,
                early,
                worker,
                late_seq: seq,
                early_seq: self.digests[early].seq,
                step: self.step,
                residual,
            });
        }

        self.executed.push(digest);
        self.executed_by_lane.entry(worker).or_default().push(digest);
        self.orphaned.remove(&digest);
        let info = &mut self.digests[digest];
        info.executed_in = Some(round);
        if let Some(stay) = info.stays.iter_mut().rev().find(|stay| stay.header == round) {
            if stay.until.is_none() {
                stay.until = Some(self.step);
            }
        }
    }

    /// Whether `late` executing after `early` is the GC-cut residual: `late` sat in an own header
    /// T when a header that drained `early` was built, that header descends from T through the
    /// own chain (so the rule let it take the lane), and T died because the GC depth cut it from a
    /// commit of its descendants.
    ///
    /// `early` then runs first either from that descendant or, when the descendant dies too (a
    /// cancel, or a later commit), because its re-queue lands ahead of T's in a separate call.
    fn residual(&self, late: DigestId, early: DigestId) -> bool {
        self.digests[early].stays.iter().any(|carried| {
            let built = self.dag[&carried.header].built_step;
            self.digests[late].stays.iter().any(|stay| {
                stay.header < carried.header
                    && stay.from <= built
                    && stay.until.is_none_or(|until| built < until)
                    && self.dag[&stay.header].death == Some(Death::GcCut)
                    && self.descends(carried.header, stay.header)
            })
        })
    }

    /// Whether the own header at `newer` reaches the one at `older` through own-parent links.
    fn descends(&self, newer: Round, older: Round) -> bool {
        if newer <= older {
            return false;
        }
        let mut round = newer;
        while round > older {
            if !self.dag.get(&round).is_some_and(|h| h.own_parent) {
                return false;
            }
            round -= 1;
        }
        true
    }

    /// The proposer handles the oldest commit notice (`process_committed_headers`).
    fn deliver_notice(&mut self) {
        self.step += 1;
        let Some((_leader, rounds)) = self.notices.pop_front() else {
            return;
        };
        let mut highest = 0;
        for round in rounds {
            highest = highest.max(round);
            // main warns when a committed round is not tracked (after a restart)
            self.proposer.tracked.remove(&round);
            if self.hold {
                self.proposer.order.forget(round);
            }
        }
        let expired: Vec<Round> =
            self.proposer.tracked.range(..=highest).map(|(round, _)| *round).collect();
        self.requeue(&expired);
    }

    /// The max-delay re-send of the newest header, while it is tracked at the proposer's round.
    fn resend(&mut self) {
        self.step += 1;
        let Some(newest) = self.newest else {
            return;
        };
        if newest != self.proposer.round {
            return;
        }
        let Some(header) = self.proposer.tracked.get(&newest).cloned() else {
            return;
        };
        self.proposer.tracked.insert(newest, header);
        if self.hold {
            self.proposer.order.record_resent(newest);
        }
    }

    /// The node restarts inside the epoch.
    fn restart(&mut self, repropose: bool) {
        self.step += 1;
        self.cancel_reports.clear();
        self.notices.clear();
        self.in_vote = None;
        let old = std::mem::take(&mut self.proposer);
        self.orphaned.extend(old.queue.values().flatten().copied());
        for (round, header) in &old.tracked {
            if !self.dag[round].committed {
                self.orphaned.extend(header.payload.iter().map(|(digest, _)| *digest));
            }
        }

        match self.newest {
            Some(newest) if repropose && newest > self.last_committed => {
                let payload = self.dag[&newest].payload.clone();
                for digest in &payload {
                    self.orphaned.remove(digest);
                }
                let lanes =
                    payload.iter().map(|digest| (*digest, self.digests[*digest].worker)).collect();
                self.proposer.tracked.insert(newest, Tracked { round: newest, payload: lanes });
                if self.hold {
                    self.proposer.order.record_resent(newest);
                }
                if !self.dag[&newest].certified {
                    self.in_vote = Some(newest);
                }
                self.proposer.round = newest;
            }
            newest => self.proposer.round = newest.unwrap_or(0),
        }
    }

    /// P3: every reported digest is in exactly one place.
    fn check_accounting(&mut self) {
        if self.accounting.is_some() {
            return;
        }
        let mut seen = vec![0_u32; self.digests.len()];
        for digest in self.proposer.queue.values().flatten() {
            seen[*digest] += 1;
        }
        for (round, header) in &self.proposer.tracked {
            if !self.dag[round].committed {
                for (digest, _) in &header.payload {
                    seen[*digest] += 1;
                }
            }
        }
        for digest in self.executed.iter().chain(&self.orphaned) {
            seen[*digest] += 1;
        }
        if let Some((digest, count)) = seen.iter().enumerate().find(|(_, count)| **count != 1) {
            self.accounting =
                Some(format!("step {}: digest {digest} counted {count} times", self.step));
        }
    }

    /// The healthy rounds P4 runs when commits lag the build by `lag` headers.
    ///
    /// The first commit (round `lag`) commits the first healed header, whose notice re-queues
    /// every older tracked header, so from round `lag + 1` on nothing is held. The queue then
    /// holds at most every digest ever reported and drains at the cap per round, and the last
    /// header carrying digests commits `lag` rounds after it is built.
    fn liveness_bound(&self, lag: usize) -> usize {
        2 * lag + 2 + self.digests.len().div_ceil(self.params.cap)
    }

    /// Run `rounds` healthy rounds whose commits lag the build by `lag` headers, and return each
    /// new header's held lanes.
    ///
    /// A healthy round delivers every pending cancel report and commit notice, certifies the
    /// header in vote collection, builds the next header on its own parent, certifies it, commits
    /// the header built `lag` rounds earlier (if there is one) and delivers that notice.
    fn heal(&mut self, rounds: usize, lag: usize) -> Vec<BTreeSet<WorkerId>> {
        let mut held = Vec::with_capacity(rounds);
        let mut healed = Vec::with_capacity(rounds);
        for index in 0..rounds {
            while !self.cancel_reports.is_empty() {
                self.deliver_cancel();
            }
            while !self.notices.is_empty() {
                self.deliver_notice();
            }
            self.build(true, true, 0, false);
            held.push(self.built.last().map(|built| built.held.clone()).unwrap_or_default());
            healed.push(self.proposer.round);
            self.certify();
            if let Some(target) = index.checked_sub(lag).map(|built| healed[built]) {
                self.commit_at(target, &[], 0, false);
                self.deliver_notice();
            }
            self.check_accounting();
        }
        held
    }

    /// Whether nothing is left waiting: the queue is empty, the tracked headers carry nothing and
    /// every digest executed or went to the orphan re-pool.
    fn drained(&self) -> bool {
        self.proposer.queue.values().all(VecDeque::is_empty)
            && self.proposer.tracked.values().all(|header| header.payload.is_empty())
            && self.executed.len() + self.orphaned.len() == self.digests.len()
    }

    /// The headers built, without the hold's bookkeeping.
    fn headers(&self) -> Vec<(Round, Vec<DigestId>)> {
        self.built.iter().map(|built| (built.round, built.payload.clone())).collect()
    }

    /// The overtakes and the headers, for a failure message.
    fn describe(&self) -> String {
        let mut out = String::new();
        for o in &self.overtakes {
            out.push_str(&format!(
                "step {}: lane {} seq {} (digest {}) executed after seq {} (digest {}), residual {}\n",
                o.step, o.worker, o.late_seq, o.late, o.early_seq, o.early, o.residual
            ));
        }
        for built in &self.built {
            out.push_str(&format!(
                "header {} payload {:?} held {:?}\n",
                built.round, built.payload, built.held
            ));
        }
        out
    }

    /// The header at `round`.
    fn dag_mut(&mut self, round: Round) -> &mut DagHeader {
        self.dag.get_mut(&round).expect("the model built this header")
    }
}

/// The proposer's state that a restart drops.
#[derive(Debug, Default)]
struct ProposerModel {
    /// The proposer's round.
    round: Round,
    /// The pending digests per lane.
    queue: BTreeMap<WorkerId, VecDeque<DigestId>>,
    /// The tracked own headers (`proposed_headers`).
    tracked: BTreeMap<Round, Tracked>,
    /// The order under test.
    order: OwnChainOrder,
    /// The drain's rotating lane cursor.
    next_lane: WorkerId,
}

impl ProposerModel {
    /// Main's drain (`drain_digests_for_header`) over the lanes not in `held`: one digest per lane
    /// per pass in ascending lane order from the cursor, first in first out, up to `cap`.
    fn drain(&mut self, cap: usize, held: &BTreeSet<WorkerId>) -> Vec<DigestId> {
        let queues = &self.queue;
        let lanes: Vec<WorkerId> = queues
            .range(self.next_lane..)
            .chain(queues.range(..self.next_lane))
            .map(|(id, _)| *id)
            .filter(|id| !held.contains(id))
            .collect();
        let max_depth = lanes.iter().map(|id| queues[id].len()).max().unwrap_or(0);
        let order: Vec<WorkerId> = (0..max_depth)
            .flat_map(|depth| {
                lanes
                    .iter()
                    .copied()
                    .filter(move |id| queues.get(id).is_some_and(|q| q.len() > depth))
            })
            .take(cap)
            .collect();
        self.next_lane = order.last().map_or(self.next_lane, |last| last.wrapping_add(1));
        order
            .into_iter()
            .filter_map(|id| self.queue.get_mut(&id).and_then(VecDeque::pop_front))
            .collect()
    }
}

/// A parent certificate as the order reads it.
#[derive(Debug, Clone, Copy)]
struct Parent {
    /// The certificate's author.
    author: u8,
    /// The digest of the certificate's header.
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

/// A tracked own header: its round and its payload as (digest, worker), in payload order.
#[derive(Debug, Clone)]
struct Tracked {
    /// The header's round.
    round: Round,
    /// The payload in order.
    payload: Vec<(DigestId, WorkerId)>,
}

impl TrackedHeader for Tracked {
    type Digest = u64;

    fn header_digest(&self) -> u64 {
        own_header_digest(self.round)
    }

    fn workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.payload.iter().map(|(_, worker)| *worker)
    }
}

/// An own header in the DAG, kept across restarts.
#[derive(Debug, Clone)]
struct DagHeader {
    /// The payload in order.
    payload: Vec<DigestId>,
    /// Whether the parents cite this node's header of the previous round.
    own_parent: bool,
    /// Whether the header has its certificate.
    certified: bool,
    /// Whether consensus committed it.
    committed: bool,
    /// The step that built it.
    built_step: usize,
    /// How it died, once it is below the last committed round without having committed.
    death: Option<Death>,
}

/// How a header that never committed fell below the author's last committed round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Death {
    /// An own-chain ancestor of a header of the commit, cut by the GC depth.
    GcCut,
    /// Not reached through the own chain: never certified, or off the chain of the commit.
    Skipped,
}

/// A reported digest.
#[derive(Debug, Clone)]
struct DigestInfo {
    /// Its lane.
    worker: WorkerId,
    /// Its sequence number in the lane.
    seq: u64,
    /// The headers it rode, in order.
    stays: Vec<Stay>,
    /// Whether a re-queue ever moved it.
    requeued: bool,
    /// The round of the header it executed from.
    executed_in: Option<Round>,
}

/// One stay of a digest in a header, from the step that put it there until the step it left.
#[derive(Debug, Clone, Copy)]
struct Stay {
    /// The header's round.
    header: Round,
    /// The step the digest entered the header.
    from: usize,
    /// The step the digest left the header (re-queued or executed).
    until: Option<usize>,
}

/// A built header.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Built {
    /// Its round.
    round: Round,
    /// Its payload in order.
    payload: Vec<DigestId>,
    /// The lanes the plan held.
    held: BTreeSet<WorkerId>,
}

/// A P1 violation: `late` executed after `early`, a newer digest of the same lane.
#[derive(Debug, Clone)]
struct Overtake {
    /// The older digest, executed second.
    late: DigestId,
    /// The newer digest, executed first.
    early: DigestId,
    /// The lane.
    worker: WorkerId,
    /// The older digest's sequence number.
    late_seq: u64,
    /// The newer digest's sequence number.
    early_seq: u64,
    /// The step of the late execution.
    step: usize,
    /// Whether it is the GC-cut residual.
    residual: bool,
}

/// One step of a schedule.
#[derive(Debug, Clone)]
enum Step {
    /// A worker reports `count` sealed batches.
    Report {
        /// The lane, taken modulo the worker count.
        worker: WorkerId,
        /// The burst size.
        count: u8,
    },
    /// The author builds its next header.
    Build {
        /// The header in vote collection is certified first; otherwise the new header cancels it.
        certify_prev: bool,
        /// The parents cite this node's previous header if it is certified.
        own_parent: bool,
        /// Rounds skipped past the next one.
        jump: u8,
        /// The cancel report is dropped (full channel, shutdown guard, no receiver).
        drop_report: bool,
    },
    /// The header in vote collection gets its certificate.
    Certify,
    /// The proposer handles the oldest cancel report.
    DeliverCancel,
    /// Consensus commits a sub-DAG holding own headers.
    Commit {
        /// Selects the newest own header of the commit.
        top: u8,
        /// Selects the older own headers another author's certificate reaches.
        picks: u16,
        /// The leader's distance above the newest own header, modulo the GC depth.
        lead: u8,
        /// The notice merges into the pending one (several sub-DAGs in one call).
        merge: bool,
    },
    /// The proposer handles the oldest commit notice.
    DeliverNotice,
    /// The max-delay re-send of the newest header.
    Resend,
    /// The node restarts inside the epoch.
    Restart {
        /// The fresh proposer re-proposes the newest header.
        repropose: bool,
    },
}

/// The fixed parameters of a schedule.
#[derive(Debug, Clone, Copy)]
struct Params {
    /// The number of worker lanes.
    workers: WorkerId,
    /// The header cap (`max_header_num_of_batches`).
    cap: usize,
    /// The GC depth.
    gc_depth: Round,
}

/// A generated schedule.
#[derive(Debug, Clone)]
struct Case {
    /// Its parameters.
    params: Params,
    /// Its steps.
    steps: Vec<Step>,
}

/// The weights of the step kinds and the odds of each choice inside a build or a commit.
#[derive(Debug, Clone, Copy)]
struct Mix {
    /// Weight of `Report`.
    report: u32,
    /// Weight of `Build`.
    build: u32,
    /// Weight of `Certify`.
    certify: u32,
    /// Weight of `DeliverCancel`.
    cancel: u32,
    /// Weight of `Commit`.
    commit: u32,
    /// Weight of `DeliverNotice`.
    notice: u32,
    /// Weight of `Resend`.
    resend: u32,
    /// Weight of `Restart`.
    restart: u32,
    /// Odds that a build finds the previous header certified.
    certify_prev: f64,
    /// Odds that a build's parents cite the own previous header.
    own_parent: f64,
    /// Odds that a build jumps rounds.
    jump: f64,
    /// Odds that a cancel report is dropped.
    drop_report: f64,
    /// Odds that a commit notice merges into the pending one.
    merge: f64,
    /// The largest report burst.
    burst: u8,
}

/// A balanced mix.
const BALANCED: Mix = Mix {
    report: 4,
    build: 4,
    certify: 2,
    cancel: 2,
    commit: 2,
    notice: 2,
    resend: 1,
    restart: 1,
    certify_prev: 0.5,
    own_parent: 0.5,
    jump: 0.1,
    drop_report: 0.25,
    merge: 0.25,
    burst: 3,
};

/// The degenerate mixes every search includes besides random ones.
const PRESETS: [Mix; 5] = [
    BALANCED,
    // no cancels: every previous header is certified before the next build
    Mix { certify_prev: 1.0, ..BALANCED },
    // all cancels: nothing is ever certified, so nothing commits
    Mix { certify: 0, certify_prev: 0.0, drop_report: 0.0, ..BALANCED },
    // the own parent always misses the parent set
    Mix { own_parent: 0.0, certify_prev: 1.0, ..BALANCED },
    // bursts of reports between headers
    Mix { report: 6, burst: 8, ..BALANCED },
];

/// One step drawn from `mix` for `workers` lanes.
fn step_strategy(mix: Mix, workers: WorkerId) -> BoxedStrategy<Step> {
    let mut options: Vec<(u32, BoxedStrategy<Step>)> = Vec::new();
    let mut add = |weight: u32, strategy: BoxedStrategy<Step>| {
        if weight > 0 {
            options.push((weight, strategy));
        }
    };
    add(
        mix.report,
        (0..workers, 1..=mix.burst)
            .prop_map(|(worker, count)| Step::Report { worker, count })
            .boxed(),
    );
    add(
        mix.build,
        (
            prop::bool::weighted(mix.certify_prev),
            prop::bool::weighted(mix.own_parent),
            prop::bool::weighted(mix.jump),
            1_u8..=3,
            prop::bool::weighted(mix.drop_report),
        )
            .prop_map(|(certify_prev, own_parent, jumps, by, drop_report)| Step::Build {
                certify_prev,
                own_parent,
                jump: if jumps { by } else { 0 },
                drop_report,
            })
            .boxed(),
    );
    add(mix.certify, Just(Step::Certify).boxed());
    add(mix.cancel, Just(Step::DeliverCancel).boxed());
    add(
        mix.commit,
        // each older own header is picked with odds 1/8, so most commits leave it out
        (any::<u8>(), any::<[u16; 3]>(), any::<u8>(), prop::bool::weighted(mix.merge))
            .prop_map(|(top, [a, b, c], lead, merge)| Step::Commit {
                top,
                picks: a & b & c,
                lead,
                merge,
            })
            .boxed(),
    );
    add(mix.notice, Just(Step::DeliverNotice).boxed());
    add(mix.resend, Just(Step::Resend).boxed());
    add(mix.restart, any::<bool>().prop_map(|repropose| Step::Restart { repropose }).boxed());
    Union::new_weighted(options).boxed()
}

/// A random mix or one of the presets.
fn mix_strategy() -> BoxedStrategy<Mix> {
    let odds = || prop::sample::select(vec![0.0, 0.25, 0.5, 0.75, 1.0]);
    let random = (
        (1_u32..=6, 1_u32..=6, 1_u32..=4, 0_u32..=4, 1_u32..=4, 1_u32..=4, 0_u32..=2, 0_u32..=1),
        (odds(), odds(), prop::sample::select(vec![0.0, 0.1, 0.3]), odds(), odds(), 1_u8..=8),
    )
        .prop_map(
            |(
                (report, build, certify, cancel, commit, notice, resend, restart),
                (certify_prev, own_parent, jump, drop_report, merge, burst),
            )| Mix {
                report,
                build,
                certify,
                cancel,
                commit,
                notice,
                resend,
                restart,
                certify_prev,
                own_parent,
                jump,
                drop_report,
                merge,
                burst,
            },
        );
    prop_oneof![3 => random, 1 => prop::sample::select(PRESETS.to_vec())].boxed()
}

/// Schedules whose mix comes from `mixes`, with the GC depth from `gc_depth`.
fn case_strategy(
    gc_depth: BoxedStrategy<Round>,
    mixes: BoxedStrategy<Mix>,
) -> impl Strategy<Value = Case> {
    (1_u16..=3, prop::sample::select(vec![1_usize, 2, 3, 10]), gc_depth, mixes).prop_flat_map(
        |(workers, cap, gc_depth, mix)| {
            prop::collection::vec(step_strategy(mix, workers), 8..=MAX_STEPS)
                .prop_map(move |steps| Case { params: Params { workers, cap, gc_depth }, steps })
        },
    )
}

/// Schedules with a GC depth that never cuts.
fn uncut_cases() -> impl Strategy<Value = Case> {
    case_strategy(Just(NO_GC_CUT).boxed(), mix_strategy())
}

/// Schedules with a small GC depth.
fn cut_cases() -> impl Strategy<Value = Case> {
    case_strategy((2_u32..=6).boxed(), mix_strategy())
}

/// Schedules with either GC depth.
fn any_gc_cases() -> impl Strategy<Value = Case> {
    case_strategy(prop_oneof![Just(NO_GC_CUT), 2_u32..=6].boxed(), mix_strategy())
}

/// Schedules whose own chain stays intact: every build finds the previous header certified and
/// cites it, and the round never jumps.
fn intact_cases() -> impl Strategy<Value = Case> {
    let mixes = mix_strategy()
        .prop_map(|mix| Mix { certify_prev: 1.0, own_parent: 1.0, jump: 0.0, ..mix })
        .boxed();
    case_strategy(prop_oneof![Just(NO_GC_CUT), 2_u32..=6].boxed(), mixes)
}

/// P1 to P4 and the residual classifier on one schedule with the hold on.
fn check_hold_on(case: &Case) -> Result<(), TestCaseError> {
    let mut model = Model::run(case, true);
    // vary the commit lag of the healthy rounds without another generated value
    let lag = case.steps.len() % 3;
    let rounds = model.liveness_bound(lag);
    let held = model.heal(rounds, lag);
    let cuts = case.params.gc_depth != NO_GC_CUT;
    prop_assert!(
        model.overtakes.iter().all(|o| cuts && o.residual),
        "P1: a lane executed out of order\n{}",
        model.describe()
    );
    prop_assert!(model.duplicates.is_empty(), "P2: executed twice: {:?}", model.duplicates);
    prop_assert!(model.accounting.is_none(), "P3: {:?}", model.accounting);
    prop_assert!(held.iter().skip(lag + 1).all(BTreeSet::is_empty), "P4: lag {lag} held {held:?}");
    prop_assert!(
        model.drained(),
        "P4: lag {lag}, not drained after {rounds} rounds\n{}",
        model.describe()
    );
    Ok(())
}

/// P5 on one intact-chain schedule.
fn check_intact(case: &Case) -> Result<(), TestCaseError> {
    let on = Model::run(case, true);
    let off = Model::run(case, false);
    prop_assert!(on.built.iter().all(|built| built.held.is_empty()), "P5: held\n{}", on.describe());
    prop_assert_eq!(on.headers(), off.headers());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// P1: with the hold on and a GC depth that never cuts, each lane's executed sequence numbers
    /// strictly increase.
    #[test]
    fn p1_each_lane_executes_in_order(case in uncut_cases()) {
        let model = Model::run(&case, true);
        prop_assert!(model.overtakes.is_empty(), "{}", model.describe());
    }

    /// P2: with the hold on, no digest executes twice.
    #[test]
    fn p2_no_digest_executes_twice(case in any_gc_cases()) {
        let model = Model::run(&case, true);
        prop_assert!(model.duplicates.is_empty(), "executed twice: {:?}", model.duplicates);
    }

    /// P3: with the hold on, every reported digest is executed, queued, tracked in a header that
    /// has not committed, or orphaned by a restart, exactly once, after every step.
    #[test]
    fn p3_every_digest_is_accounted_for(case in any_gc_cases()) {
        let model = Model::run(&case, true);
        prop_assert_eq!(model.accounting, None);
    }

    /// P4: from any state, healthy rounds (own parent present, every header certified and
    /// committed `lag` headers later) hold no lane after the first `lag + 1` and drain the queue
    /// within `2 * lag + 2 + ceil(digests / cap)` rounds.
    #[test]
    fn p4_healthy_rounds_release_every_lane(case in any_gc_cases(), lag in 0_usize..=2) {
        let mut model = Model::run(&case, true);
        let rounds = model.liveness_bound(lag);
        let held = model.heal(rounds, lag);
        prop_assert!(held.iter().skip(lag + 1).all(BTreeSet::is_empty), "held {held:?}");
        prop_assert!(model.drained(), "not drained after {rounds} rounds\n{}", model.describe());
    }

    /// P5: with the own chain intact from the start, the hold holds nothing and the headers equal
    /// the hold-off headers.
    #[test]
    fn p5_an_intact_chain_costs_nothing(case in intact_cases()) {
        check_intact(&case)?;
    }

    /// With a small GC depth, every P1 violation with the hold on is the GC-cut residual.
    #[test]
    fn gc_cut_is_the_only_overtake(case in cut_cases()) {
        let model = Model::run(&case, true);
        prop_assert!(model.overtakes.iter().all(|o| o.residual), "{}", model.describe());
    }
}

/// With the hold off, the same search finds a lane executed out of order: the model can see the
/// defect. The shrunk schedule is the issue's story, and the hold keeps that schedule in order.
#[test]
fn hold_off_model_finds_the_overtake() {
    let config = ProptestConfig {
        cases: SANITY_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    };
    let mut runner =
        TestRunner::new_with_rng(config, TestRng::from_seed(RngAlgorithm::ChaCha, &SANITY_SEED));
    let result = runner.run(&uncut_cases(), |case| {
        let model = Model::run(&case, false);
        prop_assert!(model.overtakes.is_empty(), "a lane executed out of order");
        Ok(())
    });
    let case = match result {
        Err(TestError::Fail(_, case)) => case,
        other => panic!("the hold-off model never overtook a lane, so it is too weak: {other:?}"),
    };
    let off = Model::run(&case, false);
    eprintln!("hold-off counterexample: {case:?}\n{}", off.describe());
    let overtake = off.overtakes.first().expect("the shrunk case overtakes");
    assert!(!overtake.residual, "the GC depth never cuts here");
    assert!(
        off.digests[overtake.late].requeued,
        "the older digest went back to the queue after a newer header drained its lane"
    );
    let on = Model::run(&case, true);
    assert!(
        on.overtakes.is_empty(),
        "the hold keeps the same schedule in order\n{}",
        on.describe()
    );
}

/// The default P1 run has power only if the schedules it draws break the lane order without the
/// hold: count how many of `CASES` schedules from the same strategy overtake a lane with the hold
/// off.
#[test]
fn hold_off_overtakes_often_enough() {
    let mut runner = TestRunner::new_with_rng(
        ProptestConfig::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &SANITY_SEED),
    );
    let strategy = uncut_cases();
    let hits = (0..CASES)
        .filter(|_| {
            let case = strategy.new_tree(&mut runner).expect("a schedule").current();
            !Model::run(&case, false).overtakes.is_empty()
        })
        .count();
    eprintln!("hold-off overtakes in {hits} of {CASES} schedules");
    assert!(
        hits >= MIN_HOLD_OFF_HITS,
        "only {hits} of {CASES} schedules overtake without the hold"
    );
}

/// The long hunt for the orchestrator's gate: P1 to P5 and the residual classifier over
/// `TN_ORDER_HOLD_HUNT_CASES` schedules (default 100,000) from the seed `TN_ORDER_HOLD_HUNT_SEED`
/// (default: the clock), printed before the run.
#[test]
#[ignore = "long hunt; set TN_ORDER_HOLD_HUNT_CASES and TN_ORDER_HOLD_HUNT_SEED"]
fn long_hunt_order_hold() {
    let cases = std::env::var("TN_ORDER_HOLD_HUNT_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000_u32);
    let seed = std::env::var("TN_ORDER_HOLD_HUNT_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|since| since.as_secs() ^ u64::from(since.subsec_nanos()))
                .unwrap_or(1)
        });
    println!("order hold long hunt: cases={cases} seed={seed}");
    let mut bytes = [0_u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&seed.to_le_bytes());
    }
    let config =
        || ProptestConfig { cases, failure_persistence: None, ..ProptestConfig::default() };

    let mut runner =
        TestRunner::new_with_rng(config(), TestRng::from_seed(RngAlgorithm::ChaCha, &bytes));
    if let Err(err) = runner.run(&any_gc_cases(), |case| check_hold_on(&case)) {
        panic!("order hold long hunt seed={seed}: {err}");
    }
    let mut runner =
        TestRunner::new_with_rng(config(), TestRng::from_seed(RngAlgorithm::ChaCha, &bytes));
    if let Err(err) = runner.run(&intact_cases(), |case| check_intact(&case)) {
        panic!("order hold long hunt seed={seed} (intact chain): {err}");
    }
    println!("order hold long hunt: cases={cases} seed={seed}: no schedule breaks the rule");
}

/// The parameters of the fixed schedules below: one lane, a cap of ten, a GC depth that never
/// cuts.
const FIXED: Params = Params { workers: 1, cap: 10, gc_depth: NO_GC_CUT };

/// A build that finds the previous header certified and cites it.
const BUILD_ON_PARENT: Step =
    Step::Build { certify_prev: true, own_parent: true, jump: 0, drop_report: false };

/// One report on lane 0.
const REPORT: Step = Step::Report { worker: 0, count: 1 };

/// The canceled kind: header 2 cancels header 1, so 1's digest comes back after 2 drained the
/// lane's next one. Without the hold the lane executes out of order; with it, 2 leaves the lane
/// queued and 3 carries both digests in order.
#[test]
fn canceled_story_overtakes_only_without_the_hold() {
    let steps = [
        REPORT,
        BUILD_ON_PARENT,
        REPORT,
        Step::Build { certify_prev: false, own_parent: false, jump: 0, drop_report: false },
        Step::DeliverCancel,
        BUILD_ON_PARENT,
    ];
    let case = Case { params: FIXED, steps: steps.to_vec() };
    for hold in [false, true] {
        let mut model = Model::run(&case, hold);
        model.commit_at(2, &[], 0, false);
        model.act(&Step::Certify);
        model.commit_at(3, &[], 0, false);
        assert_eq!(model.overtakes.is_empty(), hold, "hold {hold}\n{}", model.describe());
        assert_eq!(model.accounting, None);
    }
}

/// The committed kind: header 2 misses its own parent and commits alone, so header 1 dies and
/// its digest is re-queued after 2's executed. Without the hold the lane executes out of order;
/// with it, 2 leaves the lane queued.
#[test]
fn committed_story_overtakes_only_without_the_hold() {
    let steps = [
        REPORT,
        BUILD_ON_PARENT,
        Step::Certify,
        REPORT,
        Step::Build { certify_prev: true, own_parent: false, jump: 0, drop_report: false },
        Step::Certify,
    ];
    let case = Case { params: FIXED, steps: steps.to_vec() };
    for hold in [false, true] {
        let mut model = Model::run(&case, hold);
        model.commit_at(2, &[], 0, false);
        model.act(&Step::DeliverNotice);
        model.act(&BUILD_ON_PARENT);
        model.act(&Step::Certify);
        model.commit_at(3, &[], 0, false);
        assert_eq!(model.overtakes.is_empty(), hold, "hold {hold}\n{}", model.describe());
        assert_eq!(model.accounting, None);
    }
}

/// The GC-cut residual: header 2 cites header 1 and carries the lane's next digest, then a
/// leader one round above 2 with a GC depth of 2 orders 2 without visiting its parents. Header 1
/// dies, its digest runs after 2's, and the classifier calls it the residual.
#[test]
fn gc_cut_residual_is_classified() {
    let steps = [REPORT, BUILD_ON_PARENT, Step::Certify, REPORT, BUILD_ON_PARENT, Step::Certify];
    let case = Case { params: Params { gc_depth: 2, ..FIXED }, steps: steps.to_vec() };
    let mut model = Model::run(&case, true);
    model.commit_at(2, &[], 1, false);
    model.act(&Step::DeliverNotice);
    model.act(&BUILD_ON_PARENT);
    model.act(&Step::Certify);
    model.commit_at(3, &[], 0, false);
    assert_eq!(model.overtakes.len(), 1, "{}", model.describe());
    assert!(model.overtakes.iter().all(|o| o.residual), "{}", model.describe());
    assert_eq!(model.accounting, None);
}

/// The GC-cut residual through the re-queue order: header 1 is cut from the commit of its child
/// 2, then its grandchild 3 (which took the lane's next digest) dies uncertified. Two separate
/// notices re-queue 1 and then 3, so 3's digest lands ahead of 1's and header 6 carries them in
/// reverse. The classifier calls it the residual.
#[test]
fn gc_cut_residual_through_requeue_order_is_classified() {
    let cancel =
        Step::Build { certify_prev: false, own_parent: false, jump: 0, drop_report: false };
    let commit_first = Step::Commit { top: 0, picks: 0, lead: 0, merge: false };
    let steps = [
        REPORT,
        cancel.clone(),
        BUILD_ON_PARENT,
        REPORT,
        BUILD_ON_PARENT,
        // header 2 with a leader five rounds up: the GC depth of 6 cuts header 1
        Step::Commit { top: 1, picks: 0, lead: 5, merge: false },
        cancel.clone(),
        Step::Build { certify_prev: true, own_parent: false, jump: 0, drop_report: false },
        commit_first.clone(),
        Step::DeliverNotice,
        Step::DeliverNotice,
        cancel,
        Step::Certify,
        commit_first,
    ];
    let case = Case { params: Params { gc_depth: 6, ..FIXED }, steps: steps.to_vec() };
    let model = Model::run(&case, true);
    assert!(!model.overtakes.is_empty(), "{}", model.describe());
    assert!(model.overtakes.iter().all(|o| o.residual), "{}", model.describe());
    assert_eq!(model.accounting, None);
}
