# Native sender slots and validator failover

Issue [#1377](https://github.com/Telcoin-Association/telcoin-network/issues/1377) addresses
transactions occupying independently certified batches on several validators. Native sender
slots replace that selection race with a shared, consensus-ordered admission rule. RPC clients
can still submit through another validator when the current producer is unavailable.

## Activation

This is a coordinated protocol fork. `BATCH_SLOTS_FORK_EPOCH` is deliberately `Epoch::MAX`;
this PR does not schedule activation on a running network. The `test-utils` feature supports
the logged `TN_BATCH_SLOTS_FORK_EPOCH` override for isolated validation. All participating
nodes must agree on activation before native envelopes are used. Legacy behavior, including
the bounded [peer-batch deferral window](peer-batch-deferral.md), applies before activation.

## Admission and execution

The committee epoch defines one sender bucket per validator and worker. A sender maps to a
single bucket. Each bucket has a sequence, retry view, assigned producer, and canonical
execution opening. Every selected proposal or quorum timeout advances the sequence and rotates
the producer once. The view counts consecutive timeouts for the retry delay. A batch contains
transactions from one bucket and carries a BLS signature binding the chain, epoch, position,
and complete body. Retry records use the same authenticated envelope with no execution body.

Before an honest validator contributes availability, including its own stake, it validates
the envelope and persists a reservation shared by all its workers. That validator cannot
approve two different proposal bodies for the same bucket, sequence, and view, even after a
restart. Gossip prefetch only caches validated data; fresh votes over cached data still pass
the reservation barrier. Closed slot authorizations and reservations remain available for
delayed honest certification until the epoch ends.

Admission checks signatures, sender buckets, transaction types, intrinsic validity, exact
nonces, and maximum fee-plus-value affordability against the slot's pinned execution state.
A private journal advances nonces and reserves maximum costs without executing bytecode.
Only plain senders are admitted. Delegated accounts can change their own nonce again during
execution, so accepting them would invalidate the reservation model. Incoming transfers and
execution refunds cannot fund later transactions within an admission batch.

Consensus order selects the first eligible proposal or closes the sequence with a timeout
quorum. Later copies cannot produce a transaction-bearing block for a closed sequence.
The next sequence opens only after the entire selecting consensus output is
durable in execution storage and its closed authorizations are durable in consensus storage.
Original batch indices and randomness inputs remain intact when duplicate bodies are omitted.
The final selected block carries epoch closure. An output that changes only retry state gets
one execution anchor, allowing restart recovery to reconstruct exactly the published view.

## Fallback and recovery

A validator with demand may sign a timeout after a growing local delay. Local time cannot
rotate ownership: a distinct, stake-weighted quorum of timeout records must be ordered first.
The stalled sequence closes and the assigned producer changes without requiring the unavailable
producer's cooperation. Its successor uses the new execution anchor, so previously idle buckets
can recognize incoming funds instead of retaining an obsolete snapshot forever. Successors
opened inside an output cannot authorize a proposal in that same output.

Forwarding observers repeatedly queue each bucket's transactions to the current producer and
a rotating committee witness. An accepted queue entry or successful RPC response cannot end
these retries. Pending transactions remain in the pool until canonical execution removes them.
This lets honest witnesses learn demand when a destination accepts requests but censors them.
Retry progress still requires eventual delivery to honest validators and a live consensus
quorum, as does transaction inclusion elsewhere in the protocol.

Restart recovery reads canonical execution anchors and archived consensus bodies before
workers can vote. It rebuilds positions and closed authorizations, while durable vote records
prevent conflicting signatures. Native orphan recovery unwraps transaction bodies and discards
timeout payloads before returning transactions to a pool. Epoch transitions and same-epoch
mode changes preserve the publication barrier while outstanding execution drains.

## Threat model and remaining costs

Safety assumes the committee's normal Byzantine stake bound, authenticated signatures,
durable reservation storage, and deterministic archived execution reads. Availability also
requires eventual synchrony and honest delivery. Sender slots do not remove these assumptions.

The rule prevents independent validators from filling the same current slot with duplicate
transactions. It does not make every network operation fee-paying. Alternate proposals can
exist across authorized retry sequences, and their losing bodies still consume availability and
consensus resources before ordered selection. Authenticated timeout records also consume
resources. RPC spam, chosen-bucket load concentration, archival state cost, and transaction
effects on other accounts remain separate concerns. This change does not claim to eliminate
all causes of invalid transactions or underutilized execution blocks.

The admission rule is deliberately conservative: a transaction that depends on a speculative
incoming transfer waits for a later canonical opening. Bucket ownership and one outstanding
sequence per bucket also trade batching flexibility for deterministic admission. Activation
requires protocol review and network validation of these throughput and recovery tradeoffs.
