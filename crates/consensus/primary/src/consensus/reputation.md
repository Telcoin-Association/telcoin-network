# Consensus Reputation Schedule — Evaluation & Invariants Reference

This document is the single source of truth for the telcoin-network **consensus**
reputation system: how scores are earned, how they clear, and how they affect
leader election. It also records the evaluation requested by
[issue #942](https://github.com/Telcoin-Association/telcoin-network/issues/942)
("Reputation Schedule - Evaluate periodic reset"): whether the periodic reset is
appropriate given current committee/epoch semantics, and whether byzantine
behavior has a meaningful reputation impact.

Every mechanism below is stated as an assertion with a source citation. Treat
each entry as something to verify against the code.

> Scope note: this covers **consensus reputation** (`ReputationScores`), which is
> committed on-chain and drives leader scheduling. It is a distinct system from
> the libp2p **peer** reputation/penalty score
> (`crates/network-libp2p/src/peers/`, documented in `peers/penalty.md`), which
> is a local, per-node, decaying network-health score and never feeds consensus.
> Section 2.5 explains why the two must stay separate.

## 0. The one constraint that governs everything: determinism

Consensus reputation is **committed state**. The final scores are hashed into the
`CommittedSubDag` digest:

```rust
hasher.update(encode(&self.inner.reputation_scores).as_ref());
```

`crates/types/src/primary/output.rs:467` (field at `:301`, accessor at `:452-453`).

Because the scores are in the digest, and because the swap table derived from
them determines which node leads each round, **every honest validator must
compute byte-identical scores and byte-identical good/bad sets from the same
committed DAG, or the network forks.** This is not a soft performance property;
it is a safety property. It is the hard boundary on every option discussed in
Section 5:

- Reputation must be a pure, deterministic function of the **committed DAG**
  only. No wall clock, no local network observations, no libp2p peer score.
- No non-bit-portable floating point. Integer arithmetic only, except where a
  float operation is IEEE-mandated to be correctly rounded (see Section 6).
- The tuning constants are compile-time and fork-requiring; changing them
  requires a coordinated upgrade of all validators (see Section 2.2 and F4).

## 1. What the score measures

Consensus reputation is a **purely additive liveness/participation count**. There
is no negative scoring, no decay, and no penalty anywhere in the consensus path.

The one and only event that grants score, from
`Bullshark::resolve_reputation_score`
(`crates/consensus/primary/src/consensus/bullshark.rs:57-104`):

> When a sub-dag is committed, the code looks back at the **previous** committed
> leader (its digest and round). For every certificate in the committed sequence
> that is at exactly `leader_round + 1` **and** lists that previous leader's
> digest among its `header().parents()`, the certificate's author receives `+1`.

```rust
if let Some(last_committed_sub_dag) = state.last_committed_sub_dag.as_ref() {
    let leader_digest = last_committed_sub_dag.leader().digest();
    let leader_round  = last_committed_sub_dag.leader().round();
    for certificate in committed_sequence.iter().filter(|c| c.round() == leader_round + 1) {
        if certificate.header().parents().contains(&leader_digest) {
            reputation_score.add_score(certificate.origin(), 1);
        }
    }
}
```

`bullshark.rs:81-89`. `add_score` is the only mutator of a score and does only
`*val += score` on a `u64` (`crates/types/src/primary/reputation.rs:30-36`).

Consequences of this definition:

| Property | Detail | Source |
| --- | --- | --- |
| Additive only | `u64` scores, monotonically non-decreasing within a window; no subtract/decay/penalty exists | `reputation.rs:30-36` |
| Max +1 per commit | Exactly one previous leader per commit; only its round-`+1` supporters earn | `bullshark.rs:81-89` |
| Exact-round only | Only certificates at `leader_round + 1` count; transitive/late support earns nothing | `bullshark.rs:84` |
| No first-commit score | Reward loop is guarded on `last_committed_sub_dag.is_some()` | `bullshark.rs:81` |
| Semantics | High score = consistently produced a timely certificate that referenced the committed leader (liveness/connectivity). Low score = down / poorly connected / non-participating | `leader_schedule.rs:74-100` |

**What the score does NOT capture:** byzantine correctness. Equivocation, invalid
headers, and signature faults are rejected by certificate/DAG validation *before*
the commit path, so a certificate that reaches `resolve_reputation_score` is
already treated as honest. The score cannot distinguish a correct validator from
a byzantine one that still produces well-formed certificates. The only fault it
reflects is non-participation, and only implicitly (a missing `+1`).

## 2. How the score clears (the periodic reset)

### 2.1 The window

Scores are **hard-reset to zero** at the start of every schedule window. In
`resolve_reputation_score`:

```rust
let mut reputation_score =
    if (sub_dag_index / 2).is_multiple_of(self.num_sub_dags_per_schedule as u64) {
        ReputationScores::new(&self.committee)        // hard reset to all-zero
    } else if let Some(last) = state.last_committed_sub_dag.as_ref() {
        last.reputation_scores().clone()              // carry forward within the window
    } else {
        ReputationScores::new(&self.committee)
    };
```

`bullshark.rs:70-77`. There is no decay and no carry-over across the boundary:
each window starts from all-zero and accumulates independently.

`final_of_schedule` is set one commit before the reset boundary and is the flag
that tells downstream code "these are the scores to build the next swap table
from":

```rust
reputation_score.final_of_schedule =
    ((sub_dag_index / 2) + 1).is_multiple_of(self.num_sub_dags_per_schedule as u64);
```

`bullshark.rs:96-97`. The swap table is rebuilt exactly once per window, on the
`final_of_schedule` commit, immediately before the next commit zeroes the scores.

### 2.2 `sub_dag_index` is the leader nonce, so the window is epoch-phased

`sub_dag_index` is the committed leader's nonce:

```rust
let sub_dag_index = leader.nonce();
```

`bullshark.rs:218`. The nonce packs epoch and round:

```rust
((self.inner.epoch as u64) << 32) | self.inner.round as u64
```

`crates/types/src/primary/header.rs:179` (`epoch = nonce >> 32`,
`crates/types/src/helpers.rs:285`). Leaders commit only on even rounds
(`assert_eq!(leader.round() % 2, 0)`, `bullshark.rs:289`), so:

```
sub_dag_index / 2 = epoch * 2^31 + round / 2
```

The reset predicate is `(sub_dag_index / 2) % num_sub_dags_per_schedule == 0`.
With the production window of 60 (Section 2.3), and because **`2^31 mod 60 = 8`**,
each epoch increment shifts the reset phase by `8 * epoch (mod 60)` commit
indices. **Schedule windows therefore do not align to epoch boundaries.** The
behavior is fully deterministic (every node computes the same phase), but the
first window of each epoch is a different length depending on the epoch number.
See F3.

### 2.3 Production window size and the fork note

```rust
/// NOTE: Changing this value WILL REQUIRE A FORK.  All nodes must agree on the schedule change.
const CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS: u32 = 60;
```

`crates/node/src/primary.rs:28-33`, passed into `Bullshark::new` at `primary.rs:88-93`.
One window is 60 committed leaders, about 10 minutes at 10s commits. The
consensus tests use 3, 5, or 100 instead, so the production value of 60 is not
exercised on the production construction path.

## 3. What the score does: leader swap (the only consequence)

The final-of-schedule scores build a `LeaderSwapTable`
(`crates/consensus/primary/src/consensus/leader_schedule.rs:62-200`). The sole
effect of a low score is losing leadership turns. There is no slashing, no
eviction, no voting-power reduction, and no removal from quorum. A "bad" node
keeps full committee power, keeps signing certificates, and still counts toward
every `2f+1`.

Classification (`leader_schedule.rs:71-103`):

1. Sort authorities by score descending (`authorities_by_score_desc`,
   `reputation.rs:49-68`).
2. Compute integer `mean`, integer population variance, and
   `standard_dev = (variance as f64).sqrt() as u64` (`leader_schedule.rs:94`).
3. **Gate:** if `standard_dev == 0` **or**
   `lowest_rep > highest_rep - 2*standard_dev`, then good and bad sets are both
   empty and no swap ever happens (`leader_schedule.rs:101-103`). A node must be
   roughly 2 standard deviations below the top scorer before it can be tagged
   bad.
4. The bad list is capped at `bad_nodes_stake_threshold` percent of the node
   count (`DEFAULT_BAD_NODES_STAKE_THRESHOLD = 33`,
   `crates/types/src/primary/mod.rs:32`; asserted to be in `[0, 33]` at
   `leader_schedule.rs:68`).

`swap` (`leader_schedule.rs:209-230`) replaces a bad leader for a single round
with a uniformly random good node, seeded deterministically by `leader_round`.
The PRNG is `StdRng::from_seed` (see F5).

This design is deliberate. The recorded intent
(`leader_schedule.rs:74-100`) is that in a closed, permissioned validator set
with high hardware/network requirements, reputation exists "really just to filter
out down validators or nodes with a broken net connection," and the bad list is
expected to be empty in normal operation. The current std-dev heuristic is the
fix for two prior Cantina-medium findings on this exact code:
[#434](https://github.com/Telcoin-Association/telcoin-network/issues/434)
("irrecoverable reputation score trap") and
[#435](https://github.com/Telcoin-Association/telcoin-network/issues/435)
("unfair stake-based selection"). Any change proposed here must not reintroduce
either.

## 4. Relationship to committee and epochs

- The committee is fixed for an epoch. `ReputationScores::new(&committee)`
  pre-populates one zero entry per current authority (`reputation.rs:23-28`), and
  `resolve_reputation_score` asserts
  `total_authorities() == committee.size()` on every commit (`bullshark.rs:101`).
  A committee membership change at an epoch boundary therefore changes the score
  key set.
- Scores are re-loaded from store at epoch start with the current committee, so
  reputation is in practice **also reset per epoch**, in addition to the
  intra-epoch 60-commit reset.
- The intra-epoch window is not epoch-aligned (Section 2.2). This is the specific
  interaction issue #942 flags with "clarity around committee and epochs."

## 5. Findings and recommendations

Ranked by relevance to issue #942. Each finding is scoped for the determinism
constraint in Section 0. None of these are implemented by this document; it is an
evaluation to inform a maintainer decision, because every option that changes
scoring or reset cadence is a coordinated fork.

### F1 — Byzantine behavior has no reputation impact (core of #942)

**Statement.** Reputation measures liveness only. Byzantine faults are filtered
before scoring (Section 1), and the sole consequence of a low score is losing
leader turns (Section 3). A byzantine node that equivocates, censors, or
selectively withholds votes but still gets its certificates committed retains a
maximal score and full committee power.

**Evidence.** `bullshark.rs:81-89` (only additive liveness reward);
`reputation.rs:30-36` (no penalty path); `leader_schedule.rs:209-230`
(swap is the only effect).

**Recommendation.** First decide whether reputation *should* carry a byzantine
signal at all, or whether byzantine defense should stay entirely in
certificate/DAG validation plus the on-chain staking/slashing layer. If a
byzantine signal is wanted in consensus reputation, source it **only from
in-DAG, already-committed evidence** that every node observes identically, for
example a proven equivocation certificate that is itself part of the committed
DAG. Never source it from local or network-layer observations (F1 determinism).
This is a design decision, not a patch, and needs maintainer sign-off before any
code.

### F2 — The hard reset gives reputation no long-term memory (core of #942)

**Statement.** Every 60 commits the scores are zeroed with no carry-over
(Section 2.1). A node flagged "bad" in one window re-enters the next at zero and
is neutral until the next `final_of_schedule` rebuild. Persistent-but-intermittent
faulty or byzantine behavior never accumulates across resets; the same mechanism
that lets a briefly-down honest node recover also launders a repeat offender.

**Evidence.** `bullshark.rs:70-77` (hard reset), `bullshark.rs:96-97`
(single rebuild per window).

**Recommendation.** Two coherent directions, pick per the F1 decision:
(a) **Keep the hard reset.** It matches the "filter down nodes only" intent and
is the simplest safe behavior. If chosen, document it as intentional and close
the "no memory" concern explicitly.
(b) **Bounded cross-window carry.** Carry a *bounded, integer* penalty (or apply
integer decay such as halving) across window boundaries so repeat offenders
accumulate. Within-window carry already exists (the `clone` branch of
`bullshark.rs:73-74`), so intra-epoch carry is mechanically feasible. Any decay
must be integer arithmetic (F6), and cross-**epoch** carry is unsafe as written
(F3) because the committee key set changes.

### F3 — The reset window is not epoch-aligned

**Statement.** Because `sub_dag_index` is the leader nonce and `2^31 mod 60 = 8`,
the reset phase shifts 8 commit-indices per epoch (Section 2.2). Windows drift
relative to epoch starts. Separately, naive cross-epoch carry of scores is unsafe:
the first sub-dag of a new epoch generally does not satisfy the reset predicate,
so if prior-epoch scores were cloned into a changed committee the
`total_authorities == committee.size()` assert (`bullshark.rs:101`) would panic
and halt the chain.

**Evidence.** `bullshark.rs:218` (`leader.nonce()`), `header.rs:179` (nonce
layout), `bullshark.rs:70-77,101`.

**Recommendation.** For "clarity around committee and epochs," consider resetting
reputation deterministically at each epoch boundary (aligning the schedule to the
epoch), so windows are epoch-relative and the committee key set is always
consistent within a window. This is a coordinated fork and pairs naturally with
F4 (making the cadence configurable). If cross-epoch carry is ever wanted, it
must first re-key scores onto the new committee and drop departed authorities,
never `clone` blindly.

### F4 — Consensus-critical tuning constants live in code, not genesis

**Statement.** `CONSENSUS_SCHEDULE_CHANGE_SUB_DAGS = 60` (`primary.rs:33`) and
`DEFAULT_BAD_NODES_STAKE_THRESHOLD = 33` (`types/primary/mod.rs:32`) are
compile-time constants. Two validators built with different values silently fork,
and the reset cadence cannot be tuned without a hard fork (the code says as much
at `primary.rs:32`).

**Evidence.** `primary.rs:28-33,88-93`, `types/primary/mod.rs:32`.

**Recommendation.** Move both into genesis/committee config so their values are
part of coordinated, auditable state rather than binary-dependent. This is a
prerequisite for ever tuning the reset (F2/F3) safely. It is behavior-preserving
if the migrated defaults equal the current constants, but it still touches a
consensus-critical path and warrants its own PR and tests.

### F5 — `swap()` PRNG is an implicit cross-version consensus dependency

**Statement.** `swap` uses `StdRng::from_seed` to pick the replacement good node
(`leader_schedule.rs:213`). `rand` does not guarantee `StdRng`'s algorithm is
stable across major versions; a dependency bump could change the selection for
the same seed and fork validators on different builds.

**Evidence.** `leader_schedule.rs:209-230`.

**Recommendation.** Pin the PRNG to a versioned, reproducible algorithm
constructed directly (for example `rand_chacha::ChaCha20Rng::from_seed`) so the
swap choice is stable independent of the `rand` facade version. This changes the
concrete swap selection once (a one-time coordinated change) and then removes the
latent hazard permanently. Add a golden test asserting a fixed seed maps to a
fixed good-node index.

### F6 — The classification statistics are integer, but any future float is a fork risk

**Statement.** Classification uses integer mean/variance and one float:
`(variance as f64).sqrt() as u64` (`leader_schedule.rs:94`). IEEE-754 mandates
`sqrt` be correctly rounded, so this specific call is bit-portable and safe.
However, any future decay/penalty formula using a transcendental (`powf`, `exp`,
`ln`, `log2`) is libm-dependent and NOT bit-portable, and float accumulation
whose order is not pinned can diverge.

**Evidence.** `leader_schedule.rs:80-94`.

**Recommendation.** Keep all reputation math integer-only. If F2(b) decay is
adopted, implement it as integer halving or a fixed integer subtraction, never a
float formula. Treat this as a standing invariant for the subsystem.

### F7 — Test and serde coverage cannot guard a #942 change today

**Statement.** No test can even observe a byzantine penalty because no penalty
path exists. The one cross-schedule persistence test
(`test_leader_schedule_from_store`) is marked failing/TODO. The reputation serde
roundtrip builds all-zero scores and asserts only digests, so score *content* is
not verified across encode/decode even though it is in the committed digest.
There is no epoch-boundary reputation test.

**Evidence.** Reset/accrual test
`reset_consensus_scores_on_every_schedule_change` (all-honest only) at
`crates/consensus/primary/src/consensus/tests/bullshark_tests.rs:959`;
classification test `test_leader_swap_table` at
`crates/consensus/primary/src/consensus/tests/leader_schedule_tests.rs:13`;
`test_leader_schedule_from_store` at the same file `:410`, carrying
`// TODO: this test is failing` at `:408`; serde roundtrip
`test_committed_subdag_serde_roundtrip` at
`crates/types/tests/it/serde_tests.rs:62`, which builds all-zero scores
(`:73`) and never asserts score content.

**Recommendation.** Before any scoring or reset change, land the guarding tests
first: (1) a serde roundtrip that asserts non-trivial `scores_per_authority`
values and `final_of_schedule` survive encode/decode; (2) a green cross-schedule
persistence test replacing the TODO; (3) a reset-boundary test with a genuine
laggard so scores differ at the exact reset tick; (4) an epoch-boundary test
asserting scores do not leak from epoch N into epoch N+1's swap table.

## 6. Determinism and rollout constraints (guardrails for any change)

Any change to this subsystem MUST satisfy all of the following, because the
scores are in the committed digest (Section 0):

1. **Committed-DAG input only.** No wall clock, no local network/liveness
   observations, no libp2p peer score. Those are per-node local state and diverge
   immediately (fork).
2. **Integer arithmetic only**, except IEEE-mandated correctly-rounded ops. No
   `powf`/`exp`/`ln`; no order-dependent float accumulation (F6).
3. **Reproducible PRNG.** Any randomness in leader selection must be a
   version-pinned algorithm, not `StdRng` (F5).
4. **Deterministic ordering.** Selection that affects the schedule must run over
   ordered collections (the sorted `Vec`), never `HashMap` iteration order.
5. **Coordinated rollout.** Cadence/threshold constants and the scoring formula
   are effectively hard-fork parameters. Prefer moving them into genesis config
   (F4) so upgrades are explicit.
6. **Errors, not asserts, on legitimately-reachable states.** The invariant
   asserts (`bullshark.rs:101`, `leader_schedule.rs:68-69`) protect agreement by
   halting on violation. A change (for example cross-epoch carry) that could
   legitimately reach a mismatched committee must return an error and re-key, not
   assert and halt.

## 7. Summary answer to issue #942

- **How do scores clear?** Hard reset to zero every 60 committed leaders
  (Section 2), plus an effective per-epoch reset when scores reload against the
  current committee (Section 4). The intra-epoch window is deterministic but
  **not epoch-aligned** (F3).
- **Is byzantine impact meaningful?** **No.** Reputation is a liveness/participation
  count whose only effect is temporary loss of leader turns; it detects downtime,
  not malice, and the hard reset erases even that every 60 commits (F1, F2).
- **Recommended next step.** Treat F1 (should reputation carry a byzantine
  signal?) and F2 (hard reset vs bounded carry) as the design decisions to make
  first. F4 (constants to genesis) and F7 (guarding tests) are safe,
  independently-shippable prerequisites that unblock any subsequent change. All
  changes are bound by Section 6.

## Appendix — key locations

| What | Where |
| --- | --- |
| `ReputationScores` type, `add_score`, sort | `crates/types/src/primary/reputation.rs` |
| Scoring + reset + `final_of_schedule` | `crates/consensus/primary/src/consensus/bullshark.rs:57-104,218,238-267` |
| `LeaderSwapTable` classification + `swap` | `crates/consensus/primary/src/consensus/leader_schedule.rs:62-230` |
| Scores in the committed digest | `crates/types/src/primary/output.rs:301,452-453,467` |
| Window constant (fork note) | `crates/node/src/primary.rs:28-33,88-93` |
| Bad-nodes threshold constant | `crates/types/src/primary/mod.rs:32` |
| Nonce layout (epoch/round) | `crates/types/src/primary/header.rs:177-179`, `crates/types/src/helpers.rs:285` |
| Separate libp2p peer penalty system | `crates/network-libp2p/src/peers/penalty.md` |
| Prior Cantina-medium fixes on this code | issues #434, #435 |
