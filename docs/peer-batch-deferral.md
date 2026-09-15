Peer-batch deferral reduces redundant proposals after a validator receives another validator's
batch. A successfully validated batch records its transaction hashes in the worker pool. The
builder holds those transactions, and their dependent nonces, for up to ten seconds. The window
retains at most 65,536 hashes, never refreshes an entry, and forgets entries after twenty seconds.
The second half of that lifetime permits local inclusion while preventing a peer from repeatedly
rearming a deferral. An abandoned peer batch therefore cannot hold a transaction indefinitely.

This page describes the legacy mitigation. Issue #1377 also adds the coordinated
[native sender-slot fork](native-batch-slots.md), which enforces admission across validators
while preserving quorum-backed failover. Native slot safety does not depend on this cache.
That fork is inactive until the network schedules its activation epoch.

Before native activation, the mitigation depends on when the node
learns about peer proposals: two builders that select before either peer batch is validated can
both pack the same transactions. Validation afterward does not retract either proposal. If their
distinct batches reach execution, the first copy pays fees and advances the nonce; the later copy
is skipped without additional fees. A full window also ignores new hashes, making those
transactions selectable despite observation in a validated peer batch.

The `peer_batch_residuals` integration tests fix that schedule with two independent worker pools,
distinct producer beneficiaries, real peer-batch validation, and ordered engine execution. They
cover one transfer and the number of 21,000-gas transfers that fit in a batch. They check both
proposals, retained-hash telemetry, nonce-too-low skips, the empty second execution block, and the
sender's exact debit for one execution of the transfers. The builder's saturation regression uses
synthetic hashes to fill a production-capacity window that cannot expire during the test, then
checks that an overflow transaction remains selectable. This establishes the fallback behavior;
it does not measure the traffic required to saturate a deployed network.

For one set of transactions appearing once in each of K producer batches, useful transaction
execution occurs once and there can be K - 1 redundant batch copies. The two-producer tests exercise
the K = 2 schedule. This conditional accounting is not a bound over multiple rounds, repeated
proposals, or malicious committee producers. Wall-clock throughput and sustained saturation need
separate workload measurements before choosing an operational threshold.

The following metrics use only the configured worker ID as a label:

| Metric name in code | Meaning |
| --- | --- |
| `tn_peer_batch.retained_hashes{worker}` | Hashes retained across live windows for that worker, including immune entries and expired entries awaiting the next record. Clones contribute once; releasing the last clone removes that window's contribution. |
| `tn_peer_batch.insertions_dropped_total{worker}` | Unknown-hash insertion attempts discarded because the window is full. A repeated attempt for an unremembered hash counts again. Repeated known or immune hashes do not count. |
| `tn_batch_builder.peer_deferred_txs_total{worker}` | Candidate transactions the builder actually left to a peer proposal. |
| `tn_reth.invalid_txs_skipped_total{reason="nonce_too_low"}` | Execution skips, including surviving duplicates and other stale-nonce transactions. |

Prometheus exporters normalize dots in metric names to underscores. Occupancy can temporarily
exceed the per-window cap when old and new epoch windows overlap. Registration, pruning, cloning,
re-registration, and final release preserve the aggregate gauge. Counters accumulate across
epochs. Compare dropped insertions with deferred candidates and the repack monitor introduced in
#1268; nonce-too-low skips alone do not identify duplicate amplification.

Sender-slot admission at RPC ingress is a possible further mitigation. The observer forwarder's
`owning_validator` already maps the recovered sender to a committee slot, while the RPC layer
receives the underlying reth pool through `From<WorkerTxPool>`. An ingress design needs to cover
both direct submissions and forwarded submissions, epoch changes in slot ownership, and fallback
when an owner is unreachable or refuses a valid transaction. Rejecting every non-owner submission
would remove the current fallback opportunity while that owner is unavailable. Independent
forwarders can also make different availability observations, so routing preferences alone do not
provide a global uniqueness guarantee. A malicious committee producer can bypass local builder
policy altogether.

Any stronger admission rule needs an explicit availability policy and a consensus-compatible
way to enforce its intended bound. A local expiring cache cannot serve as a deterministic reason
to reject an otherwise valid certified batch. Preserve the existing memory bound, expiry, immune
window, and ability to include a transaction whose peer proposal was abandoned. The telemetry
and controlled schedules here make the residual observable while those design choices are
evaluated; they do not change transaction validity or charging rules.
