# Consensus liveness

Connection counts and traffic volume do not establish consensus liveness. Use the primary's
round gauges, quorum observations, and last successful commit handoff together. These metrics
describe one node's local view; compare active validators before diagnosing a network-wide failure.

## Metrics and observation rule

The Prometheus exporter replaces dots in Rust metric names with underscores. All metrics below
have no application labels; Prometheus supplies target labels such as `job` and `instance`.

| Prometheus metric | Meaning |
| --- | --- |
| `tn_primary_round` | Existing current primary round gauge. |
| `tn_primary_committed_round` | Existing highest committed round gauge. |
| `tn_primary_rounds_evaluated_total` | Number of distinct observed rounds finalized by this process. |
| `tn_primary_round_quorum_shortfalls_total` | Evaluated rounds below the active committee's quorum voting power. |
| `tn_primary_quorum_observation_active` | 1 while the active consensus task owns an observer, otherwise 0. |
| `tn_primary_liveness_started_timestamp_seconds` | Local Unix time when the current observer started. |
| `tn_primary_last_commit_timestamp_seconds` | Local Unix time of the last successful subdag handoff to the consensus output channel in this observer's lifetime; 0 until the first handoff. |

A certificate is observed only after the local Bullshark driver accepts it, after the epoch check.
Genesis and rounds at or below the recovered committed round are excluded. Within a round, each
current committee author contributes its `Authority::voting_power()` once, even if duplicate blocks
or equivocations are submitted. The sum is compared with `Committee::quorum_threshold()`, rather
than with a guessed number of certificates or a count of certificate signers. With equal voting
power this is `floor(2n / 3) + 1` distinct-author blocks, or `2f + 1` when `n = 3f + 1`.
The current authority API gives every member one vote. If weighted voting is introduced, the
observer already sums the API's actual voting power and uses the committee's threshold.

An observed round is finalized at the first of these boundaries serviced by the consensus loop:

1. An accepted certificate from a strictly higher round.
2. Thirty seconds after the first accepted certificate of the pending round.

The deadline does not move when more authors, duplicates, or repeated polls arrive. An ordinary
partially populated new round contributes nothing to either counter before finalization. At
finalization the denominator increases once, and the shortfall counter increases once only if
the round is below quorum. Later blocks cannot revise or reopen the result. Missing round numbers,
including large jumps during catch-up, are not counted as observed rounds.

The driver checks idle deadlines once per second. Execution or output backpressure can delay
servicing the timer while certificate processing awaits those components. The wall-clock commit
age remains visible during that delay, even if the loop stops observing rounds completely.
The thirty-second window is an observation policy, not a consensus timeout or a rejection rule.

Only one pending round's committee author set and one monotonic round watermark are retained.
Memory is bounded by the current committee size, independently of round count, round-number
jumps, duplicate traffic, and polling frequency. No addresses, peer identities, errors, URLs,
rounds, or epochs become metric labels.

## Startup, recovery and epoch transitions

- **Startup:** Before an active consensus driver starts, observation is inactive and timestamps
  are zero. Once active, the start timestamp is set and the commit timestamp remains zero until
  a successful output handoff. Give the first commit an explicit startup grace period.
- **Recovery and catch-up:** Already committed history recovered from storage does not establish
  fresh progress or populate observations. Accepted rounds above that recovery baseline are
  measured locally, including replay during active catch-up; their timing does not prove that
  the rest of the network is live. Inactive validators and followers do not run this observer.
- **Epoch changes:** Shutdown, task error, or cancellation sets observation inactive and drops the
  pending round without counting it as a failure. The next active task owns a fresh committee,
  round watermark, start timestamp, and zero commit timestamp. Old-epoch certificates cannot
  affect the new observer, even when round numbers are reused. The counters remain cumulative
  across epochs until process restart.
- **No data:** Zero evaluated rounds means no finalized sample, not zero failures. An inactive
  observer is unavailable for quorum diagnosis. A missing scrape is a monitoring failure. A live
  scrape with an active observer and an old commit/start timestamp is a potential complete stall.
- **Progress semantics:** The commit timestamp records successful consensus output channel
  handoff, not execution, durable storage, or a historical block's creation time. Monitor executed
  height separately for an execution stall. Local wall-clock corrections can distort timestamp
  ages, so keep validator and monitoring clocks synchronized.

## Dashboard and alert examples

Use one series per node. Choose operational thresholds for your deployment; the examples use a
two-minute startup/stall allowance, separate from the thirty-second observation window.

Round advancement without committing, using the existing gauges:

```promql
clamp_min(tn_primary_round - tn_primary_committed_round, 0)
  and (tn_primary_quorum_observation_active == 1)
```

Plot this beside `deriv(tn_primary_round[5m])` and
`increase(tn_primary_subdags_committed_total[5m])`. A growing gap shows round advancement
outpacing commits. A flat gap alone cannot rule out a complete stall. Round watches update
independently, so alert on sustained lag rather than a single scrape around an epoch change.

Shortfall fraction among finalized observations:

```promql
(
  increase(tn_primary_round_quorum_shortfalls_total[5m])
  / increase(tn_primary_rounds_evaluated_total[5m])
)
  and (increase(tn_primary_rounds_evaluated_total[5m]) > 0)
  and (tn_primary_quorum_observation_active == 1)
```

Display a missing fraction as "no finalized rounds", not "healthy". Display the denominator
alongside it. `increase` handles process counter resets; its five-minute window can include the
previous epoch's observations. There is deliberately no per-epoch label.

A stall after at least one commit, including when no new rounds or certificates arrive:

```promql
(time() - tn_primary_last_commit_timestamp_seconds > 120)
  and (tn_primary_last_commit_timestamp_seconds > 0)
  and (tn_primary_quorum_observation_active == 1)
```

An active task that has never committed after its startup allowance:

```promql
(tn_primary_last_commit_timestamp_seconds == 0)
  and (time() - tn_primary_liveness_started_timestamp_seconds > 120)
  and (tn_primary_quorum_observation_active == 1)
```

For alerts, use `for: 1m` to tolerate scrape jitter and brief handoff delays. Show "starting"
instead of "stalled" while the commit timestamp is zero and the start age is at most 120 seconds.
Show "inactive" for `tn_primary_quorum_observation_active == 0`. Use the scrape target's
`up == 0` alert for an unreachable process, and detect missing instrumentation on reachable
targets with a deployment-scoped expression such as:

```promql
(up{job="telcoin"} == 1)
  unless on(job, instance) tn_primary_quorum_observation_active
```

## Controlled loss and recovery evidence

The Rust test `quorum_loss_and_recovery_through_consensus_driver` drives the production
consensus task with a four-member committee and paused Tokio time. It supplies two authors
for round one, leaves the driver idle until its real observation timer records one shortfall
and one evaluated round, and verifies that no commit timestamp was fabricated. It then restores
all authors, including duplicate and late round-one certificates, verifies committed-round
advancement and a nonzero commit timestamp, and checks that the original shortfall count stays
at one. Shutdown must clear the active gauge.

The `quorum_` unit tests also pin the exact deadline, quorum threshold, duplicate identities,
weighted accumulation, round jumps, recovered-history exclusion, old-epoch rejection, rollover,
and absence of metric labels. Run the regression set with the repository's pinned toolchain:

```sh
testledger -- cargo +1.94 test --locked -p tn-primary --lib quorum_
```
