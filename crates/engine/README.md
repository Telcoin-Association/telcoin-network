# Engine

Telcoin Network engine executes consensus output to EVM-style blocks.

## Overview

Payloads correspond to a batch from the subdag that reached consensus.
Each batch is a separate payload that is executed in a separate block environment within the EVM.

Workers do not produce empty batches, but primaries always produce a `Header` for each round of consensus.
If there are no batches in the `ConsensusOutput`, a single EVM block is produced to accumulate rewards for the leader.

Block rewards are only applied to the leader for each round.

## Trust boundaries

`execute_consensus_output` (`src/payload_builder.rs`) receives a `ConsensusOutput` that Bullshark
has already committed. It re-validates almost none of it, and that is correct: every field it reads
is either covered by a quorum-signed digest or was checked once, upstream, before the digest
covering it could exist. The hazard is that the crate never said so, leaving a reader unable to tell
a deliberate trust assumption from a missing check. This section is that statement. Everything below
is derived from the code at the cited paths; when the code and this file disagree, the code wins.

Two shorthands used throughout:

- **certified** — the value is covered by `ConsensusHeader::digest_from_parts` (`tn-types`
  `primary/block.rs:44-57`), which hashes the sub-DAG digest, which hashes every committed header's
  digest (`primary/output.rs:457-472`), which hashes the header's whole serialized body including
  its payload map (`primary/header.rs:349-357`). A quorum signed that digest, so altering the value
  alters a hash a committee already voted on.
- **pre-certification only** — the check runs when a batch first reaches a worker, before its digest
  can enter a header. It is *not* re-run on the execution path: `store_synced_batches` gates
  `validate_batch` on `!is_certified` (`crates/consensus/worker/src/network/primary.rs:106`), and
  the executor's own fetch (`BatchFetcher::fetch_for_primary`) never calls it at all.

| Input (engine read) | What the engine assumes | Where the enforcing check lives |
|---|---|---|
| Batch bytes (`batch.transactions`, `:211`) | The bytes hash to the digest at the same flat index, and their size / decodability / gas / fee properties were already checked. The engine decodes nothing; recovery and execution happen in `tn-reth`. | Content binding is unconditional: `read_sync_batches` recomputes `batch.digest()` from the received frame and rejects any batch not requested or sent twice (`crates/consensus/worker/src/network/handle.rs:473-486`); the local path re-keys by recomputed digest (`crates/consensus/worker/src/batch_fetcher.rs:312`). Field validation is `BatchValidator::validate_batch` (`crates/batch-validator/src/validator.rs:39-82`), reached from `crates/consensus/worker/src/network/handler.rs:252` (gossip prefetch) and `:284` (reported batch) — **pre-certification only**. |
| Batch digests (`output.batch_digests()`, `:63`, `:183`) | The deque is the in-order concatenation of every committed header's payload keys. | Each digest is **certified** as a key of `Header::payload` (an `IndexMap`, `primary/header.rs:165`), so both its value and its position inside a header are covered. The flat deque is not itself a committed field: it is re-flattened locally by `Subscriber::fetch_batches` (`crates/consensus/executor/src/subscriber.rs:447-452`) and again, identically, by the pack-replay reader (`crates/storage/src/consensus_pack.rs:1541-1546`). The engine checks only its length. |
| `worker_id` (`batch.worker_id`, `:205`, `:238`) | Names a real worker of the authority that produced the batch. Selects the block `difficulty` low bits and the `GasAccumulator` slot the next epoch's base fee is computed from. | Three checks, none re-run here. (1) a worker rejects any batch whose `worker_id` is not its own (`crates/batch-validator/src/validator.rs:48-53`) — **pre-certification only**. (2) `Header::validate` rejects a peer header naming any `worker_id >= committee.number_of_workers()` (`primary/header.rs:138-143`); that bounds the *header's* declared id, which is **certified**. (3) header sync requires the `(digest, worker_id)` pair to exist locally before treating the batch as available (`crates/consensus/primary/src/state_sync/header_validator.rs:120`, `contains_payload`). **Nothing compares `Batch.worker_id` against the `WorkerId` the header declared for that digest**; they are bound only transitively, through (1) holding at the worker that first accepted the batch. |
| `base_fee_per_gas` (`batch.base_fee_per_gas`, `:189`) | Equals the fee the committee agreed for that worker for that epoch. Handed to `TNPayload` and used as-is — `tn-reth` never recomputes or EIP-1559-adjusts it. | `BatchValidator::validate_basefee` (`crates/batch-validator/src/validator.rs:188-196`): exact equality against the per-worker per-epoch value seeded at epoch start from the `GasAccumulator` (`crates/node/src/manager/node/start_epoch.rs:480`). **Pre-certification only.** |
| `gas_limit` | Nothing. Derived locally as `max_batch_gas(epoch)` (`:190`) — currently a constant 30,000,000 (`tn-types` `worker/sealed_batch.rs:197-199`) — so the batch's own limit is ignored at execution. | n/a. The batch's limit only ever gated the worker's own build. |
| `close_epoch` (`output.close_epoch()`, `:100`) | `true` exactly on the epoch's last output. Selects whether the epoch-close system calls run. | **Nothing, in any digest.** Derived node-locally; see below. |
| Leader identity (`output.leader().author()`, `:40`, `:120`) | The leader is in the current committee, so `RewardsCounter` can resolve its execution address. | **Certified**: a committed sub-DAG leader is a committee member by construction (`LeaderSchedule::leader` indexes `committee.authorities()`), and `Header::validate` rejects an unknown author (`primary/header.rs:133-136`). The engine adds a `TnEngineError::UnknownAuthority` fail-stop (`:139-142`) — **on the empty epoch-closing path only**; see below. |
| Batch beneficiary (`cert_batch.address`, `:197`) | Is the execution address of the certificate's author; receives priority fees. | Resolved rather than carried: the subscriber maps `header.author()` through the committee and fail-stops with `SubscriberError::UnexpectedAuthority` on a miss (`crates/consensus/executor/src/subscriber.rs:410-421`, used at `:520`); the pack-replay reader does the same with `PackError::MissingAuthority` (`crates/storage/src/consensus_pack.rs:1641`). The engine takes the resolved address on trust. |
| Commit timestamp (via `TNPayload`, becomes block `timestamp`) | Monotonic across outputs. | **Certified** — `commit_timestamp` is hashed into the sub-DAG digest (`primary/output.rs:468`). Monotonicity is imposed at construction by taking the max with the previous sub-DAG's timestamp (`primary/output.rs:367-374`). |

### The engine validates three things itself

1. **Batch/digest count** (`:66-86`), `TnEngineError::ConsensusOutputUnevenBatches`. This is the
   only guard on the flat index space, and it guards more than the payload: `get_batch_digest(i)`
   and the `close_epoch_for_last_batch(i)` boundary (`primary/output.rs:240-244`) are both indexed
   off the digest deque while the transactions are indexed off the batch vectors, so unequal lengths
   mean the epoch-close system calls fire on the wrong block or on none. Under `adiri` the check is
   relaxed for epochs `<= ADIRI_DUP_BATCH_EPOCH` (160, `tn-types` `forks.rs:46`) so testnet can
   replay a historical duplicate-batch bug.
2. **Digest index bounds** (`:182-184`), `TnEngineError::NextBlockDigestMissing`. Fires when the
   deque is *shorter* than the flattened batches. The count check above rejects that condition
   first, so this is the live guard only where the count check is relaxed — i.e. `adiri` at or below
   `ADIRI_DUP_BATCH_EPOCH`.
3. **Unknown leader fail-stop** (`:139-142`), `TnEngineError::UnknownAuthority`. **Asymmetric:** it
   guards only the empty epoch-closing path, where the beneficiary has to be derived from
   `output.leader().author()`. The non-empty path takes `cert_batch.address` (`:197`) on trust,
   because the subscriber already resolved it against the committee and fail-stopped there instead.
   The empty-path check is deliberate and argued in place (`:121-138`); it is pinned by
   `test_empty_close_epoch_unknown_leader_fail_stops` and
   `test_empty_close_epoch_without_committee_fail_stops` (`tests/it/main.rs`).

Everything else the engine reads is indexed, not checked. `output.batches()[cert_idx]` and
`cert_batch.batches[batch_idx_in_cert]` (`:185-186`) are unchecked index expressions. They cannot be
out of bounds today — both indices come from `flatten_batches()` (`primary/output.rs:194-203`),
which enumerates exactly those two nested collections. If that ever stops holding, the engine panics
in its blocking task instead of returning a `TnEngineError`; the dropped oneshot then surfaces as
`ChannelClosed` (`src/error.rs:46-50`) and halts the engine anyway.

### Digest ↔ batch positional alignment is emergent, not asserted here

The engine pairs the *i*-th flattened batch with the *i*-th digest in the deque: the digest becomes
the block's `ommers_hash` and `mix_hash = output_digest ^ batch_digest`. **This crate never verifies
that pairing** — it does not re-derive `batch.digest()` at all, only compares counts. The property
comes from two walks over the same source in `Subscriber::fetch_batches`:

- `crates/consensus/executor/src/subscriber.rs:447-452` pushes every `header.payload()` key, in
  order, across `sub_dag.headers()`, into the `batch_digests` deque.
- `:468-523` walks the same headers and the same payload keys, pushing one batch per key into that
  certificate's `batches` vector.

`Header::payload` is an `IndexMap`, so both walks see the proposer's declared insertion order, and
that order is certified (it sits inside the hashed header body). Batch bodies are content-bound to
their keys upstream by `read_sync_batches`. Hence index *i* lines up.

The property is not unguarded outside this crate:

- `test_subscriber_dup_batch_across_certs` (`crates/consensus/executor/tests/it/main.rs:578-718`)
  builds one output whose two certificates share a batch digest and asserts, for every flattened
  index, that `batch.digest() == output.get_batch_digest(index)`, and that only the final index
  closes the epoch.
- the pack-replay reader does check positions: batches must arrive in sorted-digest order and each
  is compared against the expected digest, erroring with `PackError::EpochLoad` on a mismatch
  (`crates/storage/src/consensus_pack.rs:1561-1584`).

Exactly one path breaks alignment on purpose: under `adiri` at epochs `<= ADIRI_DUP_BATCH_EPOCH`, a
duplicate digest is pushed to the deque but its batch is *not* pushed to the certificate
(`subscriber.rs:498-509`, same shape at `consensus_pack.rs:1615-1626`), shifting every later flat
index. That is the historical testnet bug being replayed deliberately, and it is why the engine's
count check needs an `adiri` escape hatch at all.

Searches behind the "not asserted here" claim, so it can be re-run: `rg 'batch_digests' --glob
'*.rs' crates/`, `rg 'digest\(\) *=='` over `crates/`, and a full read of
`execute_consensus_output`. The only positional check those turn up is the pack reader above.

### `close_epoch` is outside the consensus digest

`close_epoch` decides whether the epoch-closing system calls run for an output (`:100`, and
`close_epoch_for_last_batch` per batch). It is not a consensus field:

- `ConsensusHeader` has four fields — `parent_hash`, `sub_dag`, `number`, `extra` (`tn-types`
  `primary/block.rs:19-34`) — and its digest hashes only the first three plus a literal
  `B256::default()` (`block.rs:44-57`). `close_epoch` is not among them.
- it lives on `ConsensusOutput` itself, outside the `Arc<ConsensusOutputInner>` that the
  hand-written `Serialize` impl writes (`primary/output.rs:84-92`), and the matching `Deserialize`
  hardcodes `close_epoch: false` (`:102`). **A deserialized `ConsensusOutput` always reports
  `false`**, as that type documents in place (`output.rs:79-83`).
- every producer derives it locally as `output.committed_at() >= self.epoch_boundary`
  (`crates/node/src/manager/node/run_epoch.rs:620` and `:679-687`; also `close_epoch.rs:196`,
  `:225`, and `start_epoch.rs:100`). `committed_at()` is certified; `epoch_boundary` is the epoch
  start plus `epoch_info.epochDuration`, read from the `ConsensusRegistry` at epoch entry
  (`run_epoch.rs:150`).

So an unauthenticated boolean, computed from one certified value and one chain read, gates the
epoch-close system calls. Both inputs are chain-consistent, so honest nodes agree: this is a
**reproducibility** guarantee, not an authentication one. It is load-bearing in both directions —
one of those system calls records every worker's next-epoch base fee, and the following epoch's
entry read consumes exactly that write and halts the node when it is unreadable
(`read_base_fees_for_entered_epoch`, `run_epoch.rs:174`, `:213`). A wrong `close_epoch` does not
produce a bad block; it strands the next epoch.

Determinism rules for block production live in `crates/tn-reth/README.md` ("Determinism rules"). The
engine adds no wall-clock read and no hash-map iteration of its own.
