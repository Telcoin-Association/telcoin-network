# tn-reth

`tn-reth` is the single boundary between Telcoin Network's BFT consensus (Narwhal/Bullshark) and
reth, which TN uses as an execution library. Every read or write of EVM state by the rest of the
node goes through this crate's `RethEnv` wrapper; the goal is that no other crate touches reth
internals directly (`src/lib.rs`).

This document is written for maintainers and security researchers. Everything below is derived
from the code at the cited paths; when the code and this file disagree, the code wins.

reth is vendored at `vendor/reth` and built from source. TN's changes to it are in
`vendor/reth/CHANGES.md`; `vendor/reth/CLAUDE.md` is the quick guide for agents and reviewers.

## Execution model

- Consensus is the only block producer. Certified `ConsensusOutput` (a committed sub-DAG with
  batches) is executed by the engine (`crates/engine/src/payload_builder.rs`,
  `execute_consensus_output`), which builds one `TNPayload` (`src/payload.rs`) per batch and calls
  into `RethEnv`.
- Each batch in the output becomes one EVM block. An output with no batches and no epoch close is
  skipped entirely; an epoch-closing output with no batches still produces a synthetic closing
  block, identifiable by `ommers_hash == B256::ZERO` — the engine passes a zero batch digest for
  it (`execute_consensus_output` in `crates/engine/src/payload_builder.rs`; see the header field
  mapping below).
- Blocks are canonicalized directly: `finish_executing_output` (`src/env/execution.rs`) persists
  the blocks **and** the finalized/safe markers in a single database transaction, then broadcasts
  the canonical-state notification. There is no beacon/engine API, no fork choice, and no reorgs
  (the pool maintenance task in `src/txn_pool.rs` treats a reorg notification as an upstream bug
  and skips it).
- Transaction intake: each worker runs an independent reth transaction pool (`WorkerTxPool`,
  `src/txn_pool.rs`). Because workers batch independently, the same transaction can reach
  execution twice in one output; the executor skips `InvalidTx` failures (duplicates, bad nonce)
  and continues (`src/env/execution.rs`). Transactions whose signer cannot be recovered are
  dropped deterministically instead of halting the network (issue #933) and counted by the
  alertable `tn_reth_unrecoverable_txs_dropped_total` metric (`src/metrics.rs`).
- Peer batch deferral (`src/peer_batch.rs`, issue #1329): a batch this node validates records its
  transaction hashes in the pool's `PeerBatchTxs` window, and the batch builder skips a remembered
  hash for `PEER_BATCH_DEFER_TTL` (10 seconds), bounded by `PEER_BATCH_SEEN_MAX_TXS` hashes. A
  full window drops further hashes instead of evicting live entries, which are forgotten only at
  twice the TTL, so a flood of peer batches can switch a deferral off but never re-arm one.
  Duplicates across workers stay tolerated at execution; this only stops this node from packing a
  copy of what a peer is already proposing.
- Execution cache (`src/env/exec_cache.rs`): block execution reads accounts, slots and code
  through a cross-block cache of the values at the last built block. One hit of each kind in 256
  is re-read from the parent state; a stale one is logged, counted in
  `tn_reth_exec_cache_stale_hits_total` (alert on any nonzero value), and the block is built again
  without the cache, so that block is right and the node goes on. Unchecked stale hits of earlier
  blocks may already have put the node on another state than its peers': compare its recent block
  hashes with a peer's before trusting it, and report it. The sample follows each node's own cache,
  so nodes check different hits, and a zero on one node says nothing about another.

### Block-building timings

Block building (`RethEnv::build_block_from_batch_payload`, or the engine's
`RethEnv::recover_output_signers` followed by `RethEnv::build_block_from_recovered_payload`) times
the stages of each block it builds into six histograms (`RethEnvMetrics` in `src/metrics.rs`), one
sample per block each. Signer recovery runs per batch ahead of the build; execute, finish and
overlay extend run back to back, and hashed state and state root are parts of finish. A stage that
fails records nothing, and neither do the stages after it.

| Metric | Spans |
|---|---|
| `tn_reth_block_execute_seconds` | Creating the block builder, the pre-execution changes and the transaction loop (`src/env/execution.rs`). |
| `tn_reth_block_signer_recovery_seconds` | Recovering every transaction's signer of one batch, in parallel on rayon. Not part of execute: the engine recovers an output's batches concurrently on a background job when the output is queued. |
| `tn_reth_block_finish_seconds` | The block builder's `finish`: the post-execution changes (on an epoch-closing block, the closing system calls), merging the state transitions, hashing the post-state, the state root, and assembling the block. |
| `tn_reth_block_hashed_state_seconds` | Hashing the block's post-execution state (`hashed_post_state`). Part of finish. |
| `tn_reth_block_state_root_seconds` | The state root and trie updates over the output's accumulated overlay (`OutputTrieOverlay` in `src/env/output_overlay.rs`, with the changed storage roots on parallel tasks and the account walk split by first nibble: `parallel.rs` and `account_split.rs` there), from opening its read-only database transaction to closing it, the tasks' own transactions included. Part of finish. |
| `tn_reth_block_overlay_extend_seconds` | From the end of finish to the built block: taking the bundle, sorting the block's hashed state and trie updates, then extending the output's overlay with them. |

None of them covers the persist, which runs once per output after every block is built; reth's
`save_blocks_*` histograms time that. The Prometheus buckets for `_seconds` series (tn-metrics)
start at 5 ms, so a stage shorter than that reads only as "under 5 ms" on a dashboard.

### Parallel storage roots, account walk and storage walk

A block's state root computes the storage roots of the accounts with changed storage on rayon's
pool while the account walk runs (`src/env/output_overlay/parallel.rs`). A block with at least 64
changed accounts also splits the account walk by the first nibble of the hashed address
(`src/env/output_overlay/account_split.rs`): sixteen subtrie walks, each reth's own `StateRoot`
over cursors restricted to one nibble, run on the calling thread and pool jobs, and the calling
thread composes the root branch and the union of their trie updates. No vendored change beyond the
storage-root hook (TN-0139). A trie the split cannot compose (a small one: a few thousand accounts
or fewer) runs the walk on one thread as before.

A hot contract's storage root is split the same way (`src/env/output_overlay/storage_split.rs`): a
contract with at least 64 changed slots whose stored trie has all sixteen first-nibble nodes over
hashed children gets sixteen pieces, each reth's own `StorageRoot` over storage cursors restricted
to one nibble, worked by the same threads before the account subtries; the walk that reaches the
contract's leaf composes its storage root and the union of the pieces' trie updates. Wiped
storage, smaller tries and a split that composes nothing (an emptied storage, a single non-empty
nibble) leave the storage root to one task or the walk. On the exec bench, against the account
split alone, the state root fell to 0.53x at peak and 0.54x on the busy profile on the Mac, and to
0.41x and 0.44x on a 22-thread Linux VM. The whole output fell by 8% and 5% on the Mac (local NVMe).
On the VM's 20,000-IOPS cloud disk it fell by 9% at peak (transactions per second +10%) and not at
all on the busy profile: the busy commit is bound by the disk's IOPS, and its data sync grew by the
time the state root saved for the same pages written (both sides on one aged base, gap fill fixed at
4 pages).

The blocks whose number is a multiple of 256, of those with parallel work (storage-root tasks or a
split walk), are also computed serially and compared; a difference is logged with both roots,
counted in `tn_reth_parallel_state_root_mismatches_total` (alert on any nonzero value), and the
block uses the serial result. The sample is the block number, not a count kept by each node, so
every node checks the same blocks and nodes running the same code always publish the same root,
right or wrong. A node whose rayon pool runs one thread runs the same parallel code and composes
the same roots; only the off switches below compute serially. Any nonzero mismatch count means
the parallel code computed a wrong root, and the unchecked blocks the same bug hit carry it on every
node running this code: turn the parallel paths off on every node (`TN_PARALLEL_STATE_ROOT=0`, or
only the split the `mismatch` fallback names, `TN_PARALLEL_ACCOUNT_WALK=0` or
`TN_PARALLEL_STORAGE_WALK=0`; read at the first block, so a restart) and report it. Turning them off
on one node only parts it from its peers on the next block the bug hits. Turning them off stops the
bug; it repairs no trie node an unchecked block already persisted (the check compares the trie
updates too, so a bug can be in them). Every later root, serial ones included, is computed over
those nodes, so the nodes keep agreeing while a block's root may not be the root of its state. A
snapshot restore (`src/snapshot.rs`) finds it: it rebuilds the trie from the hashed state and fails
when that root differs from the snapshot block's header.
`tn_reth_account_walk_fallbacks_total{reason}` counts the blocks whose split walk
ran on one thread: `few_changes` and `shape` are expected (most blocks of a quiet chain, and every
split block while the trie is small), `stored_root_node` and `mismatch` (a sampled check found the
split wrong) are alerts. `tn_reth_storage_walk_fallbacks_total{reason}` counts the hot storage tries
the storage split did not use: `small_trie` (not tried) and `shape` (tried, computed serially) are
expected, `stored_root_node` and `mismatch` are alerts.
`tn_reth_parallel_state_root_task_failures_total{reason}` counts the pieces of parallel work (a
storage-root task, a walk helper, an account subtrie, a storage piece) that failed and left their
part to the serial computation, on this node only; the block also logs the first error. It is the
one per-node difference the parallel paths have, so read it first when one node parts from its
peers. `provider`, `database` and `panicked` cost the block time; `tip_moved` (a task's transaction
saw another finalized block than the walk's: something wrote the state or trie tables while an
output's blocks were built) is an alert, as the node has no such writer.

Off switches, read once at the first block from the node's environment:
`TN_PARALLEL_STATE_ROOT=0` computes every root serially; `TN_PARALLEL_ACCOUNT_WALK=0` keeps the
storage-root tasks and runs the account walk on one thread; `TN_PARALLEL_STORAGE_WALK=0` keeps every
storage root on one task. Tests and benches select the walks explicitly;
`TN_PARALLEL_STATE_ROOT_SHADOW=1` (test builds: the exec bench, tests) compares every block, split
ones included, and panics on a difference.

## Header field mapping

TN repurposes several Ethereum header fields for protocol data. Assembly happens in
`TNBlockAssembler::assemble_block` (`src/evm/block.rs`) from the `TNBlockExecutionCtx` built in
`src/evm/config.rs`; values originate in `TNPayload` (`src/payload.rs`).

| Field | TN meaning |
|---|---|
| `nonce` | `((epoch as u64) << 32) \| round` of the leader certificate (`tn-types` `primary/header.rs`; decoded by `deconstruct_nonce`). Epoch = high 32 bits, round = low 32 bits. |
| `difficulty` | `batch_index << 16 \| worker_id`. **`worker_id` is the LOW 16 bits; `batch_index` occupies the upper bits** (`TNBlockExecutionCtx` docs and `context_for_next_block` in `src/evm/config.rs`). `first_batch()` exploits this: `difficulty < 65536` ⇔ `batch_index == 0`. `worker_id_from_header` (canonical in `tn-types` `gas_accumulator.rs`, re-exported from `src/snapshot.rs`) reads the low 16 bits. |
| `mix_hash` | Computed in `crates/engine/src/payload_builder.rs` via `ConsensusOutput::prev_randao` (`tn-types` `primary/output.rs`), fork-gated on `prevrandao_seed_active` for the committing leader's epoch (`PREVRANDAO_FORK_EPOCH` in `tn-types` `forks.rs`, armed at adiri epoch 574 and conjoined fail-closed with the seed-signature fork; non-adiri builds are post-fork from genesis). Before the fork: `output_digest ^ batch_digest` when the output has batches, plain `output_digest` otherwise. From the fork epoch: `keccak256("TN_PREVRANDAO_V1" \|\| epoch seed chain value as of this commit \|\| consensus block number \|\| batch index)` (integers little-endian u64); the seed chain value advances every commit (one folded deterministic seed signature per commit), and the epoch's closing block publishes the closing commit's value as `extra_data`, a checkpoint that reproduces only that final commit's blocks. Exposed as `prevrandao` (EIP-4399). Post-fork the value cannot be ground by re-cutting the payload, but the committing leader computes every value before broadcasting and keeps one propose-or-withhold choice per commit . . . not unbiasable randomness on its own. |
| `extra_data` | Empty for a normal block. For an epoch-closing block: the 32-byte epoch-close randomness, which is epoch-gated on `seed_signature_active` (legacy keccak256 of the leader certificate's aggregate BLS signature before the fork epoch, epoch seed chain value as of the closing commit from the fork epoch on). The replay path (`context_for_block` in `src/evm/config.rs`) accepts only length 0 or 32 and errors on anything else. |
| `parent_beacon_block_root` | Digest of the `ConsensusHeader` that committed the executed transactions. Written to the EIP-4788 beacon-roots contract once per consensus output (only on the first batch, `apply_pre_execution_changes` in `src/evm/block.rs`). |
| `ommers_hash` | Digest of the executed `Batch`; `B256::ZERO` when the output carried no batches. |
| `beneficiary` | Execution address of the authority that produced the batch; receives priority fees (`src/payload.rs`, `src/evm/handler.rs`). |
| `base_fee_per_gas` | Taken from the proposed batch; never recomputed or EIP-1559-adjusted during execution (`next_evm_env` in `src/evm/config.rs` uses `payload.base_fee_per_gas` as-is). Base fees are set per worker per epoch and validated at the worker/batch level, allowing parallel per-worker base fees. |
| `gas_limit` | Taken from the worker's batch (`payload.gas_limit`). |
| `withdrawals` / `withdrawals_root` | Empty list / `EMPTY_WITHDRAWALS` on normal blocks. On an epoch-closing block the body carries one `Withdrawal` per rewarded validator whose `amount` is the validator's **consensus-leader count, not wei** (`RewardsCounter::generate_withdrawals` in `tn-types` `gas_accumulator.rs`). This is a record only: the TN block executor never credits withdrawal amounts to balances — rewards move through the `applyIncentives` system call. |
| `blob_gas_used` / `excess_blob_gas` | `Some(sum of tx blob gas)` (effectively 0, see blob handling below) and `Some(0)`. |
| `requests_hash` | Always `EMPTY_REQUESTS_HASH`; the executor returns `Requests::default()` from `finish()` (`src/evm/block.rs`) — there are no EL-triggered requests (no deposit/withdrawal/consolidation request processing). |
| `timestamp` | The sub-DAG commit timestamp from consensus, not wall clock at execution time. |

## Fork and EVM configuration

- The chain spec activates every Ethereum fork through **Prague** at genesis: block-based forks at
  block 0 and `shanghai_time`/`cancun_time`/`prague_time` at timestamp 0; `osaka_time = None`
  (`set_genesis_defaults` in `tn-types` `genesis.rs`). The chain is post-merge from genesis.
  `TnEvmConfig` (`src/evm/config.rs`) resolves the revm spec from that schedule.
- **EIP-4844 blob transactions are economically disabled, not fork-disabled.** The block
  environment prices blob gas at `u128::MAX` on every path (`TN_BLOB_EXCESS_GAS_AND_PRICE` in
  `src/evm/config.rs`, shared by `next_evm_env` for block building and execution and by `evm_env`
  for every header-derived environment: `eth_call`, tracing and block re-execution, so
  `BLOBBASEFEE` agrees between simulation, re-execution and execution), the pool's canonical-state
  update
  passes `pending_block_blob_fee = Some(u128::MAX)` (`src/txn_pool.rs`), and the batch builder
  (`crates/batch-builder/src/batch.rs`) marks blob transactions invalid via
  `BestTxns::ignore_eip4844` and purges them and their descendants with
  `WorkerTxPool::remove_eip4844_txs` (which also deletes sidecars from the blob store).
- The in-protocol `ConsensusRegistry` upgrade is gated by `CONSENSUS_REGISTRY_FORK_EPOCH` (`tn-types` `forks.rs`, armed at adiri epoch 407) and compiled only under the `adiri` feature.
  See "ConsensusRegistry fork gate" below.
- The governance-Safe fork is gated by `GOVERNANCE_SAFE_FORK_EPOCH` (`tn-types` `forks.rs`, armed at adiri epoch 554, so it fires in the closing block of epoch 553) and is also compiled only under the `adiri` feature.
  It installs the canonical Safe v1.4.1 suite and moves adiri's governance Safe proxy onto `SafeL2` (`apply_governance_safe_fork` in `src/evm/block.rs`); mainnet genesis already carries that end state, so non-adiri builds have no counterpart.
  It fails closed: if the deployed `Safe` singleton, `SafeProxyFactory` or governance proxy code does not match its pinned `*_PRE_FORK_CODE_HASH`, or the proxy's slot 0 no longer holds the L1 `Safe` singleton, the closing block aborts on every node alike.
  See step 2 of "Epoch close" below.

## Epoch close

When `ctx.close_epoch` is set (last batch of the last output in an epoch), the block executor's
`finish()` (`src/evm/block.rs`) runs this ordered sequence, **every step fatal to the block on
failure**:

1. *(adiri builds only)* If the epoch being concluded satisfies
   `concluding_epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH`, apply the registry fork
   (`apply_consensus_registry_fork`) — code swap plus one-time `migrateValidatorSets()` — so the
   remaining steps in this same block already run on the upgraded code, then the `WorkerConfigs`
   fork (`apply_worker_configs_fork`), a code-only swap with no initializer call. The two ride the
   same boundary, in that order, because the registry swap flips this very block's close onto the
   post-fork sequence, whose fourth call needs the `setWorkerConfigsData` selector the pre-fork
   `WorkerConfigs` deployment lacks — a build applying one swap but not the other aborts this
   block.
2. *(adiri builds only)* If `concluding_epoch + 1 == governance_safe_fork_epoch()`, apply the governance-Safe fork (`apply_governance_safe_fork`).
   `governance_safe_fork_epoch()` returns `GOVERNANCE_SAFE_FORK_EPOCH` (armed at 554) unless a `test-utils` build overrides it through `TN_GOVERNANCE_SAFE_FORK_EPOCH`.
   The trigger is a one-shot `==` on its own constant, independent of step 1, so it fires exactly once, in the closing block of epoch 553.
   The fork etches the eleven canonical Safe v1.4.1 contracts adiri lacks, swaps the recompiled `Safe` singleton and `SafeProxyFactory` to the canonical bytes, rewrites the governance proxy's singleton slot (slot 0) to `SafeL2` and its fallback-handler slot to the canonical `CompatibilityFallbackHandler`, and raises the Safe Singleton Factory deployer's nonce to 1 if it is lower.
   Each swap fails closed unless the deployed code hash matches its pin (`SAFE_SINGLETON_PRE_FORK_CODE_HASH`, `SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH`), and the proxy migration fails closed unless the proxy code matches `GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH` and slot 0 holds the L1 `Safe` singleton.
   State already at the fork's end (canonical singleton or factory bytes, slot 0 on `SafeL2`) also passes these gates, so re-executing the boundary over forked state is a no-op.
   The etches and the fallback-handler write are deliberately not fail-closed: an unexpected occupant is displaced and logged at `warn!` rather than halting the fleet.
   Every gate is checked against committed state before a single commit, so a failed gate aborts the block with no partial migration.
   The fork is a direct state edit, not a system call, so it spends no system-call gas and adds no `call` series to the `tn_reth_epoch_close_*` gauges.
   Nothing later in the close reads Safe state; the step sits after the registry pair and before the system calls to keep the forks-lead convention, not because of a data dependency.
3. The four boundary system calls (`apply_closing_epoch_contract_call`), in a client-enforced
   order that is itself a consensus-safety obligation: `applyIncentives(RewardInfo[])` first
   (the reward infos carry each validator's consensus-leader count, weighted on pre-slash
   collateral), then `applySlashes(Slash[])` (the slashes carrier — the protocol passes an
   empty array today), then `concludeEpoch(address[])` over a committee assembled AFTER the
   slashes commit: the eligible pool (union of `Active`, `PendingActivation`, `PendingExit`)
   is read from the registry, shuffled by a Fisher-Yates over an `StdRng` seeded with the
   epoch-close randomness, backfilled from pending-exit validators if the active set is short,
   truncated to `getNextCommitteeSize()`, and sorted by address. **The stage-ordering
   obligation lives in the client** — the registry only enforces the outcome (it validates the
   committee against post-slash state and reverts `InvalidCommitteeSize` on a stale one). An
   undersized pool fails client-side with `TnRethError::UndersizedCommittee` instead of
   submitting calldata that reverts on-chain. Finally `setWorkerConfigsData(uint16[],uint184[])`
   (`record_next_epoch_base_fees`) writes each EIP-1559 worker's next-epoch base fee into its
   `WorkerConfigs` `data` word, so a node entering the epoch reads the fee from one state slot
   instead of rescanning the prior epoch's headers. It trails the registry calls because it
   touches a different contract — nothing in the epoch transition reads what it writes — and it
   is skipped entirely when no worker is EIP-1559 (an emptiness every node derives identically
   from the same contract state, so skipping cannot diverge the fleet). While the deployed
   registry still carries the pre-fork adiri code, the close instead replays the legacy
   `applyIncentives` + `concludeEpoch(address[])` pair through the same code-hash gate as the
   committee-pool read (no interposed `applySlashes` and no base-fee record; the shared selectors
   are byte-identical), keeping pre-fork history re-executable.

There is deliberately no in-`finish()` transition merge: reth's block-builder/executor wrappers
merge once after `finish()` returns, and a second merge would push a phantom empty reverts entry
for every epoch-closing block.

System calls execute as `SYSTEM_ADDRESS` → contract with a fixed 100M gas limit, zero gas price,
base-fee and nonce checks disabled (`transact_system_call` in `src/evm/mod.rs`);
`SYSTEM_ADDRESS` is stripped from the changeset before commit so it never enters the state root.

**Slashing is not live.** `applySlashes(Slash[])` is the slashes carrier and the registry
implements it, but the client always passes an empty array (production builds);
`concludeEpoch` takes only the new committee `address[]`.

### Boundary gas

The 100M budget is granted **per call**, not per block, and is independent of the block gas limit:
a system call is submitted at `gas_price: 0`, so it neither pays for gas nor counts against the
block. That also means its gas never enters the block's `gas_used`, and no block-level or
engine-level gas metric can see it.

Every step of the close is fatal on failure, and the revert is a pure function of committed state,
so a `concludeEpoch` that outgrows its budget halts every node at the same boundary rather than
degrading. Raising the limit changes block execution and is a lockstep fleet upgrade (see the
`SYSTEM_CALL_GAS_LIMIT` doc in `src/evm/mod.rs`), so the headroom needs lead time. These gauges
(`src/metrics.rs`) are where it comes from:

| Metric | Type | Meaning |
|---|---|---|
| `tn_reth_epoch_close_system_call_gas_used{call="…"}` | gauge | Gas spent by one boundary system call. `call` is `apply_incentives`, `apply_slashes`, `conclude_epoch`, `record_base_fees`, or `registry_migration` (the one-shot registry fork migration, absent on every other close). A call that did not run publishes no series rather than a zero. |
| `tn_reth_epoch_close_gas_used` | gauge | Gas spent by those calls together. Each has its own budget, so this is the boundary's total cost rather than the bounded figure. |
| `tn_reth_epoch_close_system_call_gas_limit` | gauge | The compiled-in per-call budget, published so queries express headroom as a ratio instead of hardcoding 100000000. |

These record gas **spent**, before EIP-3529 refunds, because the spend is what the budget bounds.
That is deliberately not revm's `ExecutionResult::gas_used`, which is the spend net of the refund.
The refund is computed after execution and capped at a fifth of the spend, so it never buys the
call more room — publishing `gas_used` would slide the alert threshold with the refund: a call
earning the full refund reports four fifths of what it spent, so an 80%-of-budget alert would not
fire until the spend reached the whole 100M, which is the halt it exists to predict.

Every node records these as it executes the closing block, so the series is fleet-wide rather than
proposer-only, and it covers closing blocks re-executed during a sync — a replayed value is still a
true observation of what that epoch cost.

Dashboard — worst epoch close seen in the last 30 days, per call:

```promql
max_over_time(tn_reth_epoch_close_system_call_gas_used[30d])
```

The same figure as a fraction of the budget, which is the number to watch:

```promql
max_over_time(tn_reth_epoch_close_system_call_gas_used[30d])
  / ignoring(call) group_left max_over_time(tn_reth_epoch_close_system_call_gas_limit[30d])
```

Alert — a node's epoch close crossed four fifths of its budget in the last 30 days, the same
threshold that makes each close log a warning. `registry_migration` is excluded: it is the call the
100M headroom exists for, and it is written once at the fork block and then latches forever, so it
could never clear:

```promql
max_over_time(
  tn_reth_epoch_close_system_call_gas_used{call!="registry_migration"}[30d]
)
  / ignoring(call) group_left max_over_time(tn_reth_epoch_close_system_call_gas_limit[30d])
  > 0.8
```

Reading these correctly needs two properties of a once-per-epoch gauge:

- **They latch, they do not lapse.** The exporter registers no idle timeout, so a gauge is
  re-rendered with its last value on every scrape until the next close overwrites it. Short range
  selectors are therefore legal, but a value can be an entire epoch old, and a node that has stopped
  closing epochs keeps publishing its last figure as though it were current. Pair these with
  `tn_epoch_current` when freshness matters.
- **They are absent before the first close.** A restarted node publishes no
  `tn_reth_epoch_close_*` series at all until it next executes a closing block, which for a node at
  the start of an epoch is the whole epoch. Alerts must tolerate the absence rather than read it as
  zero.

What these do **not** cover: only the committed system calls above are instrumented. The closing
block issues other calls under their own copy of the same budget, each equally fatal. The
blockhashes (EIP-2935) and beacon-root (EIP-4788) pre-block calls are fixed-cost, so they need no
watching. The reads are the gap — `shuffle_new_committee`'s committee-pool read scales with the
eligible validator set and `record_next_epoch_base_fees`' `getAllWorkerConfigs` with the worker
count, both inherit the same 100M ceiling (contract reads share the system-call plumbing; see the
`SYSTEM_CALL_GAS_LIMIT` doc), and neither is instrumented. A boundary can still fail on a call
these gauges never showed.

## ConsensusRegistry fork gate (adiri)

`apply_consensus_registry_fork` (`src/evm/block.rs`) swaps the deployed registry bytecode in place
(preserving balance, nonce, and all storage) and runs the one-time migration. Safety properties:

- **Fail-closed code pin:** the swap refuses to run unless the deployed account's code hash equals
  `tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` (pinned to the live adiri deployment
  and guarded by an unconditional pin test in `tn-types` `forks.rs`). Migrating over an unknown
  layout would risk silent state corruption, so the block aborts instead.
- **Uniform failure:** the gate is a pure function of committed state, so every fork-capable node
  fails identically rather than diverging.
- **Byte-identical pre-fork replay:** while the registry still carries the pre-fork code hash, the
  committee-pool read speaks the legacy `getValidators(uint8) -> ValidatorInfo[]` ABI
  (`read_committee_eligible_pool_legacy`), so resync/onboarding across the fork reproduces
  historical state roots on one binary.
- **Compile-time caveat:** the whole mechanism is `#[cfg(feature = "adiri")]`. A mixed fleet of
  adiri and non-adiri builds **will diverge at the fork epoch** — non-fork builds never apply the
  swap and reject the fork block. The activation-epoch PR must ship with a confirmed fork-capable
  build on every validator (rollout notes in `tn-types` `forks.rs`).

## Fee economics

All fee handling lives in `TNEvmHandler` (`src/evm/handler.rs`); system calls bypass it entirely.

- **Base fees are credited, not burned.** `reward_beneficiary` sends the priority-fee portion
  (`effective_gas_price - basefee`) × gas used to the block beneficiary and the base-fee portion
  × gas used to the chain's base-fee address for later off-chain processing.
- **Quadratic gas-limit penalty** (`calculate_gas_penalty` in `src/evm/utils.rs`, referenced from
  `reimburse_caller`; see issue #424). Because actual gas cannot be known until after consensus,
  users who submit gas limits far above usage could stuff batches for free. The penalty: zero when
  `gas_limit <= 210_000` or usage ≥ 10% of the limit; otherwise
  `penalty = ((10^8 - usage_ratio_scaled)^2 × unused_gas) / 10^16` in deterministic u128 integer
  math. The penalty is deducted from the caller's unused-gas refund and credited to the base-fee
  address. Penalty is computed from pre-refund gas so SSTORE refunds don't inflate it.
  User-facing documentation (formula, examples, detection recipe for wallets and integrators):
  [`docs/src/gas-penalty.md`](../../docs/src/gas-penalty.md) at the repo root. Keep the two in sync.
- **`BASEFEE_ADDRESS` is a process-global `OnceLock`** (`src/lib.rs`), written once by
  `set_basefee_address` during `RethEnv::new` (`src/env/mod.rs`). The first write wins; later
  writes are **silently discarded** (the `set` error is intentionally ignored). If it is never
  set, `basefee_address()` falls back to `GOVERNANCE_SAFE_ADDRESS`. Every node in the fleet must
  agree on this value — a node with a different base-fee address computes different state roots
  and forks off (the doc comment on `set_basefee_address` says exactly this).
  `Parameters` (`tn-config`) declares `basefee_address` as a required key with no serde default,
  so a `parameters.yaml` that lost the key fails at parse time with an error that names the field
  instead of reaching this fallback in silence.

## Precompiles

`TNEvmFactory` (`src/evm/factory.rs`) installs both TN precompiles on **every** EVM instance it
creates — both `create_evm` and `create_evm_with_inspector`, which covers block production,
replay, and read-only/RPC EVMs alike:

- **TEL precompile at `0x…07e1`** (`TELCOIN_PRECOMPILE_ADDRESS`,
  `src/evm/tel_precompile/mod.rs`): native token issuance operating directly on native account
  balances — timelocked `mint`/`claim` (7-day `TIMELOCK_DURATION`), `burn`, and `totalSupply`,
  gated to the governance safe. Full details in
  [`src/evm/tel_precompile/README.md`](src/evm/tel_precompile/README.md).
- **BLS G1 verify precompile at `0x…b151`** (`BLS_G1_PRECOMPILE_ADDRESS`,
  `src/evm/bls_precompile/mod.rs`): `blsVerify(bytes,bytes,bytes)` verifies a 48-byte compressed
  G1 signature under a 96-byte compressed G2 pubkey using the same `blst` (`min_sig`) path the
  consensus layer signs with, for 150,000 gas plus 12 gas per 32-byte word of the ABI-encoded
  arguments, charged before the arguments are decoded. `ConsensusRegistry` staticcalls it for
  proof-of-possession checks; the message is otherwise opaque.
- Both precompile accounts carry a single `0xfe` (INVALID) byte of genesis code
  (`PRECOMPILE_GENESIS_BYTECODE`, `src/system_calls.rs`) so they are never state-pruned and any
  call bypassing precompile dispatch reverts.

**Feature-flag hazard:** the `faucet` feature replaces the timelocked mint with an instant,
role-gated `mint(address,uint256)` and **must never ship in mainnet builds**. Note
`adiri = ["faucet", "tn-types/adiri"]` in `crates/tn-reth/Cargo.toml` — building for adiri
transitively enables faucet minting. At startup a faucet build refuses mainnet (chain id 731) and
the embedded mainnet genesis, and starts on any other chain but adiri only with
`--allow-faucet-on-chain` (`validate_faucet_build` in `telcoin-network-cli`).

## Trust boundaries

### reth-side validation is a fail-loud stub

`TNExecution` (`src/traits.rs`) implements reth's `HeaderValidator`, `Consensus`,
`FullConsensus`, and `PayloadValidator` traits, and **every method returns `Err`**. TN never
drives reth's beacon-engine validation or payload conversion — consensus output is already final
and blocks are canonicalized directly — so these impls exist only to satisfy trait bounds when
wiring the RPC stack (`src/env/rpc.rs`). Any call means that machinery was wired in by mistake,
and the error surfaces it immediately instead of silently skipping validation or fabricating a
block (issue #1048).

### reth networking is disabled entirely

`RethConfig::new` (`src/cli.rs`) zeroes out reth's devp2p stack: discovery disabled, port 0,
no peers, tx gossip disabled, propagation `Max(0)`, identity "Reth Null Network". All real
networking is TN's own libp2p; the reth payload builder and pruning are likewise unused.

### RPC surface

- **Namespace allowlist** (`src/cli.rs`): the constant `ALLOWED_MODULES` permits exactly
  `eth, net, web3, debug, trace, rpc`. `RpcModuleSelection::All` is rewritten to
  `DEFAULT_MODULES` (`eth, net, web3, rpc`): the expensive `debug` and `trace` namespaces are
  never part of `all` and are enabled only when named explicitly, which warns. Explicit
  selections are intersected with the allowlist, and every dropped module (`admin`, `txpool`,
  or an unrecognized name) is warned about by name. Applied to HTTP and WS from the CLI
  flags; the IPC transport has no selection flag and serves the same `DEFAULT_MODULES` set
  (`src/env/rpc.rs`). The TN-specific `tn_*` module is outside this allowlist and is
  registered on every enabled transport.
- **Transport limits** (`src/rpc_server_args.rs`, CLI-overridable defaults): 500 max connections,
  15 MB max request, 160 MB max response, 1024 subscriptions per connection, plus reth's default
  `eth_call` gas cap, no RPC transaction fee cap, tracing/filter/proof limits, and
  an eth-proof window.
- The RPC stack is built over the worker's pool and the `WorkerNetwork` shim (`src/worker.rs`),
  which serves `net`/`web3` info from TN's libp2p peer count. No engine/auth namespace exists.
  A failure to merge the TN-specific RPC module is logged at `error!` but does not stop the
  server (`src/env/rpc.rs`).

### Transaction forwarding (observer → committee)

Non-committee nodes forward RPC-submitted transactions to committee validators' **advertised
JSON-RPC endpoints** (`WorkerRpcForwarder`, `src/forward.rs`). Routing pins each sender to one
validator slot (first 8 bytes of the sender address, little-endian, mod committee size) so nonce
ordering converges, with fallback to every other advertised endpoint. Bounds: 5s per endpoint,
15s total per transaction. Error classification matters: transient codes (`-32003` full pool,
`-32603` internal) fall through to the next validator; "already known" counts as delivered; any
other JSON-RPC rejection is treated as a considered verdict and **stops the fallback chain** —
no other validator is tried. The advertised endpoint may be `http` or `https`
(`RpcInfo::validate` in `tn-types` `committee.rs` accepts both), so raw signed transactions can
transit plaintext HTTP if a validator advertises it; TN adds no authentication or encryption of
its own on this path.

The dial target is likewise chosen by a committee member rather than by this node, so
`ForwardTargetPolicy` gates it at the point an advertised URL becomes a provider. `PublicOnly`
(the default) refuses loopback, private, link-local, unique-local, shared-address-space and
unspecified IP literals in every spelling, plus the names reserved to resolve locally, and logs
each refusal once at `warn` with the advertising validator's BLS key. Operators of single-host
deployments opt back in with the `allow_private_forward_targets` parameter. The check never
resolves DNS, so a hostname that resolves to an internal address is still dialed.

### Operator-configured submission forwarding

`--forward-txs` (`src/rpc_server_args.rs`) turns a node, normally a public RPC observer, into a relay for raw transaction submissions.
`get_rpc_server` (`src/env/rpc.rs`) then installs `EthSubmitForwarded` (`src/rpc_tx_forward.rs`) in place of the fee-cap guard on every transport that serves `eth`.
`eth_sendRawTransaction` runs the local precheck and forwards the client's bytes to an ordered list of validator RPC targets; the local pool is never touched, so the node-record path above never sees the transaction.
`eth_sendRawTransactionSync` forwards a plain `eth_sendRawTransaction` and waits for the receipt on this node's canonical stream, so a target only ever receives `eth_sendRawTransaction`.
Operator documentation: `docs/src/getting-started/public-rpc-nodes.md`.

What the node trusts and what it guards:

- The targets come from local configuration, not from a committee member, so `ForwardTargetPolicy` does not apply and a private address is a normal target.
- The target list is private configuration. `ForwardTarget` and `TxForwardConfig` redact it in `Debug` (`src/rpc_tx_forward/target.rs`), the startup `info!` logs only the target count, and the forwarder's `debug!` lines and the `tn_reth_rpc_tx_forward_target_failures_total` metric name a target by its index. The HTTP connector underneath (hyper-util `client::legacy::connect::http`) does log each new connection's resolved `ip:port` at `debug`, which the default `--log.file.filter debug` writes to the log file; stdout logs it too when it runs at `debug` (`-vvvv` or `RUST_LOG`); adding `hyper_util::client::legacy::connect=info` to both `--log.file.filter` and `--log.stdout.filter` keeps it out. That is hygiene: the firewall in front of the target, not the secrecy of its address, is what protects it. When no target answers, the client receives one fixed error, `-32603` `transaction submission unavailable` with no `data`, which carries neither a target nor timing detail.
- A target's reply is the client's answer. A hash or a JSON-RPC error object (code, message, raw `data`) passes through unchanged and ends the attempt; the node does not check that the hash matches the bytes it sent. A compromised target can therefore answer clients with anything an RPC endpoint could, so a node trusts its targets as far as a client trusts the validator behind them.
- Failover happens only below the JSON-RPC layer: a failed connection, no reply within `ATTEMPT_TIMEOUT` (5 s, including the wait for a request slot), a non-2xx status other than 413, a body over `MAX_RESPONSE_BYTES` (64 KiB), or a 2xx body that is not a JSON-RPC reply. Each such failure demotes the target for `DEMOTION_COOLDOWN` (30 s); when that runs out, one submission probes the target in its configured place while the others keep trying it last. An attempt the budget cuts short of `ATTEMPT_TIMEOUT` neither demotes nor counts against its target. One submission spends at most `SUBMIT_BUDGET` (15 s), each target holds at most `MAX_INFLIGHT_PER_TARGET` (128) requests, and a list holds at most `MAX_TARGETS` (8) entries (`src/rpc_tx_forward/client.rs`, `target.rs`).
- The forwarded request is capped at the node's own `--rpc.max-request-size` plus `REQUEST_SIZE_HEADROOM` (1 KiB), since the rebuilt envelope can be a few bytes larger than the request the node accepted. A request that is still too large (refused by that cap, or answered with HTTP 413 by a target with a lower limit) ends the submission with the oversized-request error `-32007`, counted as `upstream_error`, and charges no target, rather than being uploaded to every target in turn.
- Transport is jsonrpsee's `HttpClient`, with rustls for `https://` targets. A plain `http://` target carries signed transactions in cleartext, and URL userinfo with a password goes out as a basic-auth header; a username without a password is refused at startup, since it would be dropped. TN adds no authentication or encryption of its own.
- Without `--sanitize-txs`, the only local check is the `--rpc.txfeecap` guard, which with the default cap of 0 decodes nothing, so any bytes within the request size limit reach the target. With it, the precheck decodes the transaction, recovers the signer, applies the batch type allowlist (`tn_types::batch_allowlisted_tx_type`) and checks the chain id, and refuses with reth's own error objects before any target sees the transaction.
- On a committee member the flag is not refused: the node logs one `warn` per process (`crates/node/src/manager/node/start_epoch.rs`), and that node's RPC submissions bypass its own pool.
- One `Arc<TxForwarder>`, built in `RethEnv::new`, is shared by every worker's RPC server, so all lanes share one target order, one demotion state, one connection pool and one metrics set. Nothing is epoch-scoped, no task is spawned, and clients connect lazily, so an unreachable target does not fail startup.

### `SYSTEM_ADDRESS` is unreachable by users

`SYSTEM_ADDRESS` (`0xff…fe`, `src/system_calls.rs`) is crate-private and has no known private
key, so no signed transaction can recover to it. It appears as `caller` only in internally
constructed system-call transactions: the block executor's state-changing epoch calls and the
read-only registry/config queries (`src/evm/block.rs`, `src/env/epoch.rs`). State-changing
callers strip it from the changeset before commit, and `TNEvmHandler` skips fee accounting for
it so the beneficiary and base-fee accounts are never spuriously touched.

### Snapshot export / restore

`src/snapshot.rs`. Export (`PinnedStateView`) streams plain state from a pinned read-only database
transaction into a CRC32+zstd state pack; it records the caller-supplied state root **without
recomputing it**. The restore side is the real integrity check (`SnapshotRestorer`):

- refuses any datadir that already holds chain data; genesis (the trust root) always comes from
  the local chain spec, never the snapshot;
- scaffolds a header-only chain (real hashes inside the shipped window, zero-hash dummies below);
- **recomputes the state root from scratch** out of the imported accounts (`StateRoot::from_tx`)
  and hard-fails unless it equals both the pack's declared root and `header(B).state_root` from
  the caller-supplied header window;
- **validates that `B` closed an epoch** instead of assuming it (`entry_readiness_precondition`):
  the registry pinned at `B` must report the entered epoch beginning at `B + 1`, since a restored
  node seeds its first epoch entry from one pinned read at `B` and starts its accumulator catchup
  at `B + 1`;
- requires the `WorkerConfigs` read at `B` to report at least one worker and to yield a readable
  entry base fee for every one of them — an `Eip1559` `data` word wider than `u64` is refused here,
  naming the worker and the word, instead of halting the restored node at its first epoch entry;
- persists finalized/safe markers at `B` and re-checks the reconstructed tip number and hash.

What restore does *not* verify: the authenticity of the header window itself — that is the
caller's trust decision (the window is validated upstream against consensus data).

A restored node did not execute `B`, so its epoch-record check (see "Rebuilding in the new layout"
below) skips the one record whose closing block is `B`: when the node enters the epoch after it,
the restored epoch's record is not compared with `B`. `db load-state` already bound that record's
`final_state` to the pack's final block. The restored-state floor marker (`restored-state-floor`)
is what tells the node `B` was imported; every epoch it closes above `B` is compared.

## Database backend

TN's reth execution database lives in `<datadir>/db`. The default build runs it on the tn-lmdb
engine (`tn_lmdb_reth::TnLmdbEnv`, `db/tn-lmdb.dat`); every release, mainnet's included, is this
build. The opt-in `mdbx` feature (forwarded by `tn-node`, `telcoin-network-cli` and
`telcoin-network`) runs it on MDBX instead (`reth_db::DatabaseEnv`, `db/mdbx.dat`): the legacy
backend, kept until testnet has resynced, for the tests of that path (`./etc/ci-lanes.sh test-mdbx`, run by hand: not in CI or the attestation)
and as the previous binary of the migration e2e lanes. reth is not patched: it is generic over the
database on every path TN uses, and MDBX stays compiled in both builds (reth's crates require it,
and the default build reads an MDBX database to migrate it). Every backend difference lives in
`src/env/database.rs`; other code names `RethDbEnv` and `RethDb` (`Arc<RethDbEnv>`).

- **Open** (`open_rw`, called by `RethEnv::new_database`): the MDBX build opens through reth's
  `init_db` with `with_metrics`. The tn-lmdb build maps the `--db.*` flags, checks reth's
  `database.version` file before anything else (below), runs the backend guard, records its own
  version in the file, opens `TnLmdbEnv`, creates reth's tables and records the client version in
  `VersionHistory` as reth's `record_client_version` does.
- **Version file** (`check_db_version_file`, `record_db_version_file`, tn-lmdb build):
  `db/database.version` holds `TN_LMDB_DB_VERSION` (1002: reth's version 2 plus 1000), never
  reth's own. It is a rollback tripwire: a binary that runs reth on MDBX alone (a release from
  before the backend guard, `v0.16.0-adiri` among them) checks the file before it opens anything
  and stops with "breaking database change detected: your database version (v1002) is
  incompatible with the latest database version (v2)", where it would otherwise create an empty
  `mdbx.dat` beside `tn-lmdb.dat` and prune the static files down to it. The check runs before the
  backend guard, so a directory another binary wrote (any other version, or a file that does not
  parse) is refused before a migration touches it. The migration records 1002 before it removes
  MDBX's files, both in the run that copies and in a later start that finishes a stopped
  migration. A missing file, or one holding reth's 2 (a tn-lmdb `db/` from before the tripwire),
  is rewritten atomically after the guard. A new `db/` is made durable in its parent.
- **Backend guard** (`check_backend`, both builds): the MDBX build refuses a `db/` holding
  `tn-lmdb.dat`, and the tn-lmdb build migrates one holding `mdbx.dat` (below). Opening the wrong engine
  would create an empty database beside the datadir's static files, and reth's
  `check_consistency` would then prune the static files back to the empty database's tip. The
  checks stat each name with `symlink_metadata` (a symbolic link counts as present, dangling or
  not), so a stat error fails the open. The MDBX build's error names the
  directory and both engines; the operator runs the default build or starts from a fresh datadir
  (or `db load-state`). The MDBX build also refuses a migrated `db/` whose `tn-lmdb.dat` is
  missing (the marker `db/mdbx-migrated` alone), saying to restore the file and run a build
  without `mdbx`, rather than create a new database in its place. It warns of a stopped migration's temp
  directory (`db/tn-lmdb-migrating/`, a partial copy beside an unchanged MDBX database), naming it
  and its size, and never removes it.
- **MDBX to tn-lmdb migration** (`src/env/database/migrate.rs`, tn-lmdb build): the first open of a
  `db/` holding `mdbx.dat` moves it onto tn-lmdb ("Migrating from MDBX" below).
- **`--db.*` flags on tn-lmdb** (`tn_lmdb_args`): `--db.max-size` is the mapping's address-space
  reservation (unset: the engine's default, 1 GiB); past it the engine remaps at double the size,
  so it is not a cap, and the node does not stop at it as reth's help for the flag says (that
  describes MDBX): the node logs a warning when the flag is set (`warn_max_size_is_a_reservation`,
  where the command line is read, so a temp chain's own reservation draws none).
  `--db.page-size` sets a new file's page size (the engine accepts a power of
  two in 4 KiB..=64 KiB). `--db.growth-step` is the largest growth step (unset, the engine's 1
  GiB): the file grows by doubling up to it, then by it, each step preallocated, and a clean close
  gives back the unused tail; a step under the engine's minimum (`MIN_GROWTH`, 1 MiB) is refused.
  `--db.gap-fill <PAGES|auto>` (TN's own flag, `RethCommand::db_gap_fill`, carried as
  `RethConfig::db_gap_fill`, a `GapFill`; unset: `auto` on Linux, off on macOS and on the MDBX
  build (`DEFAULT_DB_GAP_FILL`); `off` or 0: off; a width of at most the engine's
  `MAX_GAP_FILL`, 64, else refused naming the flag) is the engine's `EnvOptions::gap_fill`: a
  durable commit first rewrites each gap of at most that many clean pages between two runs of its
  dirty pages with the bytes they hold, so the disk takes fewer, larger writes for more bytes
  (`crates/tn-lmdb/README.md`, "Gap fill"). A width suits disks that cap I/Os per second (cloud
  block storage), not local NVMe. `auto` chooses each scattered commit's width from the disk's
  measured sync costs (README, "Automatic width"): it settles at 0 where filling does not pay and
  fills where writes are capped; its choice and fit are the `tn_lmdb_gap_auto_*` database
  counters (`DbCounters`; the width, the fitted cost per write and per page, the fill's cost per
  gap and the capped flag are gauges, `NOT_DIFFERENCES`). The MDBX build refuses any but off.
  `--db.sync-mode durable` (the default) is durable commits. `--db.sync-mode safe-no-sync` is the
  engine's `NoSync` with automatic syncs (`SAFE_NO_SYNC_PERIOD`, 1 s, and `SAFE_NO_SYNC_BYTES`, 64
  MiB): a commit returns before it is durable, and the first commit a second after the last durable
  state, or whose pages bring those written since to 64 MiB, commits durably with every commit
  before it, and so does the first commit that changes something after each open of the database
  (the open is not a durable state). Every other commit writes the engine's weak meta (no sync), so
  a crash of the process alone loses nothing, but for one inside the 128-byte copy of a meta, which
  rolls back to the last durable state; a system crash or a power loss rolls back to the last
  durable state.
  Neither corrupts the file, and the pages commits free wait at most about one of those intervals
  and a commit, so the file stays bounded (an engine `NoSync` with no syncs would grow until one: see
  "Persist benchmark"). Refused at startup, naming the flag: `--db.exclusive`,
  `--db.max-readers`, `--db.log-level`, and reth's
  `UtterlyNoSync` and `NoMetaSync` sync modes (neither has a CLI value). The `UtterlyNoSync`
  refusal says why and points at `safe-no-sync` and `durable`: the engine has no mode that never
  syncs.
- **Default page size** (`default_page_size`): the page size of a new database without
  `--db.page-size`. The MDBX build's is reth's own default, the OS page size clamped to 4 KiB..=64
  KiB (16 KiB on Apple Silicon, 4 KiB on x86 Linux), which its `open_rw` passes to MDBX explicitly;
  the tn-lmdb build's is the adapter's 4 KiB. The execution bench keys its cached bases by it.
- **Check** (`db_check`, behind `telcoin-network db check`, tn-lmdb build): the engine's full
  check of the database's newest committed state (`TnLmdbEnv::check`: every page's CRC and
  structure, every tree and overflow run, each table's counts, the exact accounting of the file's
  pages), which reads never run (a read checks a page's bounds and identity, its CRC only before
  a copy or a reuse: tn-lmdb's README, do-not-flag item 3). Opens read-only through the same
  state checks as the stats, with the node stopped (`DB_CHECK_NEEDS_STOPPED_NODE`; `db check`
  holds the datadir lock). Damage is an error naming the directory and what failed, and the file
  is never changed. The MDBX build refuses it: MDBX keeps no page checksums. Run it after a
  migration (which runs the same check itself), after a disk or memory scare, and before trusting
  a restored file.
- **Table stats** (`db_table_stats`, behind `telcoin-network db stats`): each of reth's tables the
  database has, sorted by name, with its rows, branch, leaf and overflow pages, and size (the page
  size times its pages, `DbTableStat`). A table the database lacks is skipped. The MDBX build
  opens read-only through reth's `open_db_read_only`, which reads while the node runs, after the
  backend guard's checks (refusing a `db/` holding `tn-lmdb.dat` or the marker `db/mdbx-migrated`).
  The tn-lmdb build opens `TnLmdbEnv` read-only and reads the adapter's `table_stats`, which needs
  the node stopped (`DB_TABLE_STATS_NEEDS_STOPPED_NODE`): a database a node holds is refused with
  "in use: stop the node first", and `db stats` holds the datadir lock for the report. It never
  migrates: a `db/` holding `mdbx.dat` is refused, as the open refuses it beside the marker or
  beside a `tn-lmdb.dat` no migration left (neither the marker nor the emptied
  `db/tn-lmdb-migrating/`), and otherwise as a migration pending or unfinished, which the node's
  start runs. With neither `mdbx.dat` nor `tn-lmdb.dat` there is no reth database.
- **reth's dup-sort tables on tn-lmdb** (the adapter's crate docs, "Dup-sort tables"): the state
  tables (`PlainStorageState`, `HashedStorages`, `StoragesTrie`) are the engine's dup tables, each
  key's rows packed in its outer cell, or in a nested tree of their own once they outgrow it, so a
  seek bounded to an account's or a trie's rows never reads the next key's; the changesets
  (`AccountChangeSets`, `StorageChangeSets`), appended by block and read by range, are composite keys (`key ‖ subkey`) in plain engine tables. A
  table's form is part of the file's format: the open refuses a file holding one in the other
  form (`MDBX_INCOMPATIBLE`, naming the table and both forms). No tn-lmdb file in the other form
  existed outside tests before tn-lmdb became the default (it was pre-release and off by default
  until then), so there is no migration between the forms.
- **What differs on tn-lmdb:**
  - no per-operation latency metrics (MDBX's `with_metrics`). The sampled gauges keep MDBX's
    names (`db.table_size` and the rest), so dashboards and the e2e `reth_db_table_size` check
    hold;
  - a read transaction past `--db.read-transaction-timeout` (MDBX's five minutes by default) is
    marked, not aborted: its later calls fail with `ReadTransactionTimeout` as on MDBX, and its
    pages return to reuse when reth drops it. The snapshot export (`RethEnv::pin_state_view`) and
    the migration opt out (`disable_long_read_transaction_safety`), as on MDBX, and the adapter's
    long-reader warning can fire, at most once a minute, during a long export: the export holds
    back page reuse while it runs, so the file grows until it ends.

### Migrating from MDBX

The default (tn-lmdb) build that finds an MDBX reth database (`db/mdbx.dat`) migrates it at
the node's first start, when `RethEnv::new_database` opens the database and before reth reads
anything. The module docs of `src/env/database/migrate.rs` have the states and steps. The node
logs `mdbx.dat`'s size, the live bytes of reth's tables in it and the free space, each table's
copy, and at the end the rows copied and the time taken. It warns of any table in MDBX that is not
one of reth's: its rows are not copied (reth does not read them either).

- **What it does:** it copies every one of reth's tables raw (encoded keys and compressed values; no
  row is decoded) into a new tn-lmdb file built in `db/tn-lmdb-migrating/`, with the node's `--db.*`
  settings (`--db.page-size`, `--db.max-size` and `--db.growth-step`). Before anything of MDBX's is
  touched it checks each table's rows against MDBX's count, a read-back hash of each table against
  the copy, and the synced file against the engine's full check. It opens MDBX read-only and
  exclusively (`MDBX_EXCLUSIVE`): while another process has the MDBX database open (an MDBX build
  on a second datadir naming the same `db/`, writing or idle), MDBX refuses the open with
  `MDBX_BUSY` and the migration does not start, and while the migration holds it open, from the
  copy until MDBX's files are removed, MDBX refuses any other process's open. It takes a record of
  `mdbx.dat` when it opens MDBX (MDBX's last transaction id, and the file's length and
  modification time), and takes it again once the copy is checked, with MDBX still open: a
  difference means something wrote MDBX during the migration past the exclusive open (a write to
  the file itself, or a process MDBX's file locks do not reach), and the copy misses those
  writes, so the migration fails, leaving MDBX as the write left it. Then it writes the record to
  `db/tn-lmdb-migrating/mdbx-source`, renames the file to `db/tn-lmdb.dat` (the commit point),
  checks that `tn-lmdb.dat` opens and that `mdbx.dat` matches the record (see "A stop part way"),
  records the tn-lmdb version in `db/database.version` (the rollback tripwire, above), removes
  `mdbx.lck` and then `mdbx.dat`, closes MDBX, writes the marker `db/mdbx-migrated` (its text
  lists the rows copied per table) and removes the temp directory. reth's static files are not touched. While `mdbx.dat` or the temp directory is
  there, the open holds an exclusive `flock` on `db/` itself (two datadirs can name one `db/`, and
  the datadir lock is one datadir's), so a second process migrating or finishing the same
  directory is refused with "another process is migrating this database"; the open looks at
  `db/`'s files under that lock, so a migration another process finished just before is seen as
  finished. The copy needs no case
  of its own for the dup-sort tables'
  forms: MDBX walks a key's values in subkey order, so each row is an
  append of `key ‖ subkey` in order, a key's rows going to its packed set or nested tree on a dup
  table, also when they span the copy's commits
  (`a_dup_key_with_more_rows_than_a_commit_migrates_whole`).
- **Free space:** the filesystem holds both databases while it copies. The migration refuses to
  start unless the live bytes of reth's tables in MDBX plus a quarter plus 1 GiB (or the growth
  step, `--db.growth-step`, when that is larger) are free (`needed_bytes`), and says the live bytes,
  how much is needed and how much is free. The live bytes are each table's branch, leaf and overflow
  pages times the page size, not `mdbx.dat`'s file size, which can be far more: MDBX grows the file
  in large steps (a local node's is 4 GiB, mostly sparse) and keeps the pages it frees. `db stats`
  on the MDBX build (the build being replaced) prints the same figures beside a running node, each
  table's "Total Size" and their sum in the "Tables" row, so the space can be checked before
  upgrading. The quarter is a margin, not a measurement: the copy's tn-lmdb tree need not take
  the pages MDBX's did (each engine fills and splits its leaves by its own rules, and keeps a
  value in the leaf up to its own limit, tn-lmdb's about half a page, the rest in overflow runs
  of whole pages), the file also holds its catalog and free tree, and the filesystem needs room
  to work. The on-demand test below prints the copy's table pages against the live bytes, with
  their ratio, and `the_migrated_tables_fit_the_headroom` holds the test database within the
  margin. The check is an early refusal: a disk that fills during the copy fails the migration
  the same way, leaving MDBX as it was.
- **Time:** in proportion to the database's size: one read of MDBX, one write and one read-back of
  the copy, and one sync. The on-demand test `migrates_a_copy_of_a_real_database` (in
  `src/env/database/migrate/tests.rs`; its doc comment has the command) migrates a copy of a real
  `db/` and checks every table, printing the live bytes and the free space needed. On 2026-10-27
  it migrated a small local-validator database (2110 rows; `mdbx.dat` a 4 GiB file whose tables
  take 704512 bytes of pages, so the migration needed 1074622464 bytes free, where the file's size
  would have asked for 6 GiB) in 45 ms on macOS, debug build, into a 1.0 MiB `tn-lmdb.dat`. A
  production-sized database has not been timed yet.
- **A stop part way** (a kill, a power loss) is safe at every step. Before the commit point MDBX is
  as it was, and the next start removes the temp directory and migrates again from the beginning.
  After it, the next start finishes the remaining steps as the first run does: it records the
  tn-lmdb version, removes what is left of MDBX's files and writes the marker. It knows a stopped
  migration by the temp directory, emptied by the rename and removed only after the marker. Before
  it changes anything it checks three things, and refuses with both files kept if one fails:
  `tn-lmdb.dat` opens read-only (a stopped migration of an older build may have left a tn-lmdb
  format this build does not read); no other process has MDBX open (it opens MDBX exclusively, as
  the migration does, and holds it until MDBX's files are removed); and `mdbx.dat`'s length and
  MDBX's last transaction id equal the record the migration took when it opened MDBX. A stop
  between the rename and the version file leaves a window in which a build running reth on MDBX
  alone opens and advances `mdbx.dat`, and removing it then would silently roll those writes
  back. The modification time is not compared: an MDBX commit syncs data, not the file's times, so
  after a power loss the time on disk can differ with no write. A temp directory without the
  record (an older build's migration) is not compared. A failure after the commit point (the
  database directory's fsync, or a finishing step) keeps the temp directory too, and its error
  says to start this build again, which finishes the migration, and not to start an older release. An MDBX build started after a stop before the commit point opens MDBX as
  it was and warns of the temp directory, naming it and its size, but never removes it: remove it
  by hand to free the space (a tn-lmdb build's next start removes it and migrates again).
- **Refusals.** The node does not start, and nothing is changed:

  | the node finds | why | what to do |
  |---|---|---|
  | less free space than the migration needs | the copy is built beside MDBX | free space, then start again |
  | `db/` locked by another process's migration ("another process is migrating this database") | two datadirs name one `db/`, and one node is migrating or finishing it | wait for it to finish, then start again |
  | an MDBX database another process has open, which MDBX refuses the migration's exclusive open with `MDBX_BUSY` ("another process has the MDBX database open") | two datadirs name one `db/`, and an MDBX build runs on the other, committing or idle; the migration would remove the `mdbx.dat` it writes to | MDBX is unchanged: stop that process, then start again |
  | an MDBX database MDBX refuses to open read-only with `MDBX_WANNA_RECOVERY` (stopped uncleanly under `--db.sync-mode safe-no-sync`) | a read-only open does not recover it; the error says what to do | MDBX is unchanged: start the previous binary (the one that ran reth on MDBX) once and stop it cleanly, then start this build |
  | a row tn-lmdb cannot store: a second value for a dup-sort subkey (`MDBX_KEYEXIST`), or a value over 16 MiB (`MDBX_BAD_VALSIZE`) | tn-lmdb stores one value per subkey and values up to 16 MiB; reth writes neither | MDBX is unchanged: run the MDBX build, and report the error |
  | a copy that fails its checks (a short read, a read-back mismatch, the engine's check) | the copy would lose or change rows | as above |
  | an `mdbx.dat` whose last transaction id, length or modification time after the copy differs from the migration's record of it at the open ("another process wrote the MDBX database during the migration") | something wrote MDBX while it was copied, past the exclusive open (a write to the file itself, or a process MDBX's file locks do not reach), and the copy misses those writes | MDBX is as that write left it: stop the writer, then start again |
  | `mdbx.dat` beside `db/mdbx-migrated` | an MDBX build (an older binary) wrote MDBX after the migration, and the static files may no longer match `tn-lmdb.dat` | rebuild the execution database: `telcoin-network db reset-execution` (it moves `db/` and `static_files/` aside); do not start an older release on the datadir |
  | `mdbx.dat` beside `tn-lmdb.dat`, with neither `db/mdbx-migrated` nor the emptied `db/tn-lmdb-migrating/` | not a stopped migration: say, a tn-lmdb database an MDBX build (an older binary) opened, creating `mdbx.dat` beside it; the static files may match either file | as above |
  | `mdbx.dat` beside `tn-lmdb.dat` and the emptied `db/tn-lmdb-migrating/`, differing in length or MDBX transaction id from the record `db/tn-lmdb-migrating/mdbx-source` | an MDBX build (an older binary) wrote MDBX between the migration's commit point and its version file; removing `mdbx.dat` would roll those writes back | both files are kept. When the error says MDBX is at a later transaction than the migration copied, the MDBX build advanced it with the static files: remove `db/tn-lmdb.dat` and `db/tn-lmdb-migrating/`, then start this build again, which migrates `mdbx.dat` from the start. Otherwise (`mdbx.dat` may be one an MDBX build created anew, pruning the static files down to it) rebuild as above. Do not start an older release on the datadir |
  | an unfinished migration (the emptied `db/tn-lmdb-migrating/` beside `tn-lmdb.dat`) whose `tn-lmdb.dat` this build cannot open, say in another tn-lmdb format version | a stopped migration of an older build | nothing is changed. With `mdbx.dat` still there: remove `db/tn-lmdb.dat` and `db/tn-lmdb-migrating/`, then start again, which migrates MDBX from the start. Without it: rebuild as above |
  | an unfinished migration whose `mdbx.dat` another process has open (`MDBX_BUSY`, "another process has the MDBX database open") | two datadirs name one `db/`, and an MDBX build runs on the other | both files are kept: stop that process, then start again |
  | `db/mdbx-migrated` without `tn-lmdb.dat` | the migrated database is missing | restore `tn-lmdb.dat`, or start from a fresh datadir (or `db load-state`) |
  | an MDBX database whose `storage_settings` row or TN layout marker says v2, does not parse, or names an unknown format version (`refuse_non_v1_source`) | the migration changes the engine, never the layout, and no TN build writes v2 on MDBX | MDBX is unchanged: resolve by hand |

- **No rollback:** once `mdbx.dat` is removed there is no way back to MDBX. The MDBX build of this
  tree refuses a `db/` holding `tn-lmdb.dat`, or the marker without it (saying to restore the file
  and run a tn-lmdb build), so going back means a fresh datadir (or `db load-state`). A release
  from before the backend guard has no such refusal; what stops it is the version file: it
  refuses `db/database.version` (1002, the tn-lmdb version) before it opens anything, so `db/` and
  `static_files/` are left as they were and the tn-lmdb build still runs on them. Do not edit or
  delete the file to get past it: the old binary would then create an empty MDBX database beside
  `tn-lmdb.dat`, prune the static files down to it, and fail at genesis. Back up `db/` and
  `static_files/` before the first tn-lmdb start of a datadir. Problems are fixed forward, as for
  the consensus DB.
- **`db migrate-execution`** (`migrate_reth_db`) runs the migration offline, with the node stopped,
  so the first start of the new binary does not wait on it: it takes the datadir lock and the
  node's `--db.*` flags (pass what the node runs with, so the file gets the same page size,
  reservation and growth step), runs the version check, the migration (or finishes one stopped
  after its commit point, or refuses what the node's start refuses) and the version file, as the
  node's open does before it opens, and prints the marker's text (the rows migrated per table)
  without opening the database. With no MDBX database it has nothing to do and creates nothing.
  The MDBX build refuses it.
- **`db stats`** never migrates: it refuses a `db/` holding `mdbx.dat` until a node start (or
  `db migrate-execution`) has run the migration.
- **`db load-state`** never migrates either: it refuses a `db/` holding `mdbx.dat`, whatever MDBX
  holds, or an unfinished migration (the emptied `db/tn-lmdb-migrating/` beside `tn-lmdb.dat`,
  which the node's next start finishes), before anything opens it (`refuse_pending_migration`);
  it restores into a fresh datadir.

### Storage layouts

reth has two storage layouts (`StorageSettings`). **v1** keeps change sets, transaction senders and
the history indices in database tables, and plain state beside hashed state. **v2** (here without
RocksDB) moves change sets and senders into static files and keeps only hashed state; its storage
change sets and history are keyed by the plain slot, as v1's are (vendor/reth CHANGES.md TN-0112).
v2 runs only on tn-lmdb. Nothing converts a datadir from one
layout to the other: flipping the recorded layout alone would misread every history index, so a
datadir moves to v2 by a rebuild (below). `src/env/layout.rs` holds the rules.

- **Recorded twice.** reth's `Metadata` row `storage_settings` (a missing row reads as v1), and TN's
  marker `tn_storage_layout`, JSON `{"version":1,"layout":"v1"}`. The marker's version is TN's
  format version of the reth database: a later change to how TN stores a table (packed trie keys, a
  separate transaction-hash index) takes the next version, and an older binary refuses it rather
  than misread it.
- **A new datadir** gets `StorageLayout::for_new_datadirs()`: always v1 on the MDBX build, and
  `NEW_DATADIR_LAYOUT_TN_LMDB`, v2, on the tn-lmdb build (the v2 write path keeps the history
  indices and the transaction-hash index in the database: vendor/reth CHANGES.md TN-0110..0112).
  `the_default_build_gives_new_datadirs_v2` pins both, through the node's own config, and
  `etc/backend-gate.sh` (run by `./etc/ci-lanes.sh test-default`) fails if any crate turns the
  `mdbx` feature on in the release graphs or that lane. Benches and tests pick a layout with
  `RethConfig::with_new_datadir_layout`; in test builds `TN_TEST_NEW_DATADIR_LAYOUT=v1` (or `v2`)
  picks it for every temp chain (`TEST_NEW_DATADIR_LAYOUT_VAR`), which `test-default` uses to run
  tn-reth's and tn-engine's suites a second time on v1, the layout migrated datadirs keep; the
  node's own path never reads it. The marker commits before genesis, and genesis writes
  `storage_settings` with the layout (`init_genesis_with_settings`).
- **An existing datadir** opens in its stored layout, never flipped. When it differs from the layout
  new datadirs get, every start logs a warning naming `db reset-execution`; the node runs as before.
  A datadir from before the marker (every one is v1) gets the marker at its first open.
- **Checked at every open**, before reth's consistency check (which prunes static files ahead of the
  database), with the start refused. A refused start leaves the static files and the database's
  chain as they were, but the database's own open has run first and left what any open leaves: a
  `db/` that was missing or empty now holds `database.version` and a database without a chain
  (reth's tables, empty; on the tn-lmdb build `tn-lmdb.dat`, with `database.version` at 1002), the
  client version is added to `VersionHistory` when it differs from the last row, and on the
  tn-lmdb build an MDBX database has been migrated:

  | the node finds | why |
  |---|---|
  | v2 on the MDBX build, for a new datadir or an existing one | v2 is tn-lmdb only |
  | a `storage_settings` row or marker that does not parse, or a marker of another format version | reth's own read takes an unparseable row for v1; a newer binary may have written the datadir |
  | a marker whose layout disagrees with `storage_settings` | the row was changed without converting the data |
  | a v2 datadir without the marker | no TN build wrote it |
  | a v1 datadir whose static files hold a segment only v2 writes (senders, change sets) | another datadir's static files: a half-done swap |
  | a v2 datadir whose static files hold headers but no senders segment, which v2 writes from genesis | a v1 datadir's static files: the other half-done swap; v2's consistency check would prune them to this database |
  | static files holding headers beside a database without a chain | `db/` was removed or replaced; reth's consistency check would prune the static files down to it |

  One state beside a database without a chain is healed instead: a creation stopped inside
  genesis's commit. Genesis finalizes its static files before it commits the database, so a stop
  between the two leaves TN's marker (committed first) and no `storage_settings` row in the
  database, beside static files holding only this chain's genesis block. The open removes those
  static files (headers last, so a stop part way is found again) and creates the datadir anew
  (`holds_only_genesis`). With no marker, or anything past genesis, it refuses as above.

  Once the layout is settled, reth's consistency check (`check_consistency`) prunes static files
  ahead of the database's stage checkpoints (a stop between a persist's two commits). The other
  direction, the database ahead of the static files (an unwind target: static files lost, removed
  or swapped for an older copy), is refused too: TN has no stage pipeline to unwind the database,
  so the start stops with "storage consistency check found the database ahead of the static
  files", saying to restore the datadir from a snapshot or backup.

- **Logs:** `new reth datadir: storage layout` or `reth datadir: storage layout` at each start, with
  the layout; an existing datadir in the other layout logs the warning `this reth datadir keeps its
  storage layout` with `layout` and `new_datadirs` instead.
- **Static-file checksums (v2).** A v2 datadir's new static files carry a CRC-32 on every value,
  on every `.csoff` record (which block owns which change-set rows) and on each `.conf` (vendor/reth
  CHANGES.md TN-0120..0124; the v1 tables are covered by tn-lmdb's page CRCs, and v1 static files
  keep upstream's format byte for byte). Every read verifies before it decodes, and the change-set
  lookups behind the pinned committee and epoch reads verify the row they return. A mismatch is an
  error, never a value: `static file <path> is damaged: the value at row <r>, column <c> fails
  its checksum`, `changeset offsets <path> are damaged: record <i> fails its checksum`,
  `static file configuration <path> is damaged: it fails its checksum`, or
  `changeset static file is corrupted` (a `.csoff` range past its rows, or a sidecar the startup
  heal will not cut). It reaches the epoch reads as a node-local provider fault
  (`StateReadError::Provider`): retried briefly, then the node stops instead of using any other
  value (`damaged_v2_changeset_fails_pinned_reads_closed`). RPC reads of the damaged rows fail;
  others answer. Startup opens each segment's latest file to heal an interrupted write, and
  refuses that file, rather than cutting it, when its `.csoff` holds a committed record that fails
  its checksum, or when it or its `.csoff` is shorter than its committed configuration (a prune
  commits the configuration before it shrinks a file, so only damage leaves that); older files are
  verified as they are read. Startup also refuses a segment whose files skip blocks between two of
  them (a file missing: `<segment> static files skip blocks`). **Operator response:** the file is damaged on this node only (disk or memory fault,
  or a bad copy), and no heal can rebuild it. Stop the node, keep the datadir for diagnosis if
  wanted, and rebuild the execution database: `telcoin-network db reset-execution`, then start
  the node, which re-executes the chain from genesis (or restore a snapshot). Check the disk:
  one damaged file usually means more. A v2 datadir written before these checksums does not open
  (`static file <path> has no checksums`): v2 datadirs must hold only checksummed files, since
  two flipped bits could otherwise pass a checksummed file off as an unchecked one. Recreate it,
  or `db reset-execution` and re-execute.

### On-disk format versions

Every format TN writes carries a version marker, and a binary refuses every version it does not
read. A change to the bytes a marker covers bumps that marker; nothing else tells old bytes from
new ones.

| marker | covers | bump it when |
|---|---|---|
| `TN_LAYOUT_VERSION`, in `Metadata["tn_storage_layout"]` (`src/env/layout.rs`) | how TN stores the reth database: every table's key and value codec (reth Compact or raw, including the dup-sort subkey widths), the codecs of the static-file values (header, transaction, receipt, sender, change-set entry), which tables and static-file segments a layout writes, and how static files commit | a codec or key form changes (packed trie keys), data moves to another store (the transaction-hash index, history shards), a new static-file segment, or segment configuration moves into the database commit |
| jar version, the first 8 bytes of each `.conf` (vendor/reth nippy-jar) | a jar's framing: value and `.csoff` checksums, the `.conf` fields with `SegmentHeader`, the compressor, the row layout | a jar's layout changes (chunked change sets, fixed-width senders): the next version, 3. `SegmentHeader` has no spare field, so a new field needs it too |
| tn-lmdb `FORMAT_VERSION`, in the meta record, checked before its CRC (`crates/tn-lmdb/src/meta.rs`) | pages, overflow runs, catalog, free list, meta | any engine format change; never by a flag bit alone, since unknown flags read as damage |
| `TN_LMDB_DB_VERSION`, `db/database.version` (`src/env/database.rs`) | the engine the `db` directory holds (1002: reth-on-MDBX binaries refuse it) | the backend changes |
| `EXEC_STATE_PACK_VERSION` (`crates/storage/src/exec_state_pack.rs`) | the state-pack export peers exchange | the export's records change |

- **A new static-file segment, or segment configuration moved into the database commit, bumps
  `TN_LAYOUT_VERSION`.** An older binary silently skips the files of a segment it does not know
  (`iter_static_files`, vendor/reth/crates/storage/db/src/static_file/mod.rs: `parse_filename`
  gives `None`), and one that does not know configuration held in the database heals a stale
  `.conf` and cuts rows the database committed. Only the marker, checked before any heal, stops it.
- **reth's compiled-in zstd dictionaries** for transactions and receipts never change: a stored
  value records that it is compressed, not with which dictionary.
- **Two releases.** Release N reads the new format (and the old); release N+1 writes it. A
  one-release rollback then still opens every datadir; a release that writes what its predecessor
  refuses turns a rollback into a resync.

**Format-freeze fixtures** (`src/env/format_freeze_tests.rs`). `tests/fixtures/format-freeze/`
holds a v1 and a v2 tn-lmdb datadir built from one fixed chain (`genesis.json` and the module's
plan: legacy, EIP-2930, EIP-1559 and EIP-7702 transactions, contract creation, an account removal,
an epoch close, a two-block output), each with its digests (`digests.txt`).
`format_fixtures_read_back_unchanged` opens a copy of each with the current code and compares every
table row, static-file row, provider read and the state-pack export.
`format_writer_output_unchanged` builds the chain again and compares each tn-lmdb table's raw rows
and each static file's bytes (the decompressed rows of the LZ4 headers jar), the version markers
and the block hashes; it is ignored on the adiri build, whose fork schedule builds other blocks.
Both run in the normal lanes in well under a second. When one fails:

- an unintended change is a bug: fix the code, never the fixtures;
- an intended format change bumps its marker, moves each old fixture to its own directory (for
  example `v2` to `v2-layout1`) with a test of what the new binary does with it (opens, converts or
  refuses it), and then regenerates. A format no release has written has no datadirs to keep
  reading, so its fixtures are regenerated in place: tn-lmdb's format 7 (tree tags) replaced
  format 6, and format 8 (overflow-run tags) replaced 7, before tn-lmdb shipped, and the
  engine's own tests pin the refusal of every earlier version (`a_version_7_record_is_refused`
  and its siblings in `crates/tn-lmdb/src/meta.rs`;
  `a_version_4_file_is_refused_at_open` in `crates/tn-lmdb/src/env/tests.rs` opens a whole file);
- a block-hash difference is an execution change, not a format change (check `TN_*_FORK_EPOCH` in
  the environment first).

Regenerating is explicit, on the default build:

```sh
TN_REGENERATE_FORMAT_FIXTURES=1 cargo test -p tn-reth --features test-utils --lib \
    env::format_freeze_tests::regenerate_format_fixtures -- --ignored --exact
```

It keeps `genesis.json` (delete it to rebuild it from the current contracts) and rewrites `v1/` and
`v2/`. `tn-lmdb.dat` differs at every run (transaction ids, boot id); everything else is
byte-identical, so review the `digests.txt` diff: every changed line must follow from the intended
change.

### Upgrading a testnet node

A testnet node on `v0.16.0-adiri` runs MDBX for both databases, in storage layout v1. The first
start of this release's binary (the default build, tn-lmdb):

1. migrates the reth database from MDBX to tn-lmdb ([Migrating from MDBX](#migrating-from-mdbx)):
   the engine changes, the layout stays v1, and the node records the layout marker. The node opens
   the reth database before it touches the consensus database, so this step runs first, and can
   run alone: to run it before the start, with the node stopped, run
   `telcoin-network db migrate-execution` with the node's `--db.*` flags; the start then finds a
   tn-lmdb database;
2. migrates the consensus database from MDBX to tndb and removes the MDBX environments (one-way,
   #1558);
3. runs on. Nothing else is needed: a v1 datadir keeps working on this binary.

**Upgrade every node together.** Since #1788 a node record carries a network-key co-signature, and
this release drops the records `v0.16.0-adiri` publishes (and the old binary cannot use the new
ones): in an e2e committee started on `v0.16.0-adiri`, a validator restarted on this release
migrated both databases but could not resolve its peers, and stalled. So a mixed committee does not
work; and an observer on this release cannot sync from a network still on the old one. Upgrade all
nodes in place first (they keep v1), and rebuild in the new layout only after that.

**What runs this upgrade.** No automated lane starts from `v0.16.0-adiri`. The e2e lanes'
previous binary (`make test-e2e-reth-migration`, `make test-e2e-resync`) is this tree's `mdbx`
build, which already keeps the consensus database in tndb and records the layout marker, so step 1
and the marker recording of a datadir from before the marker never run there. Unit tests cover
both: `tn-storage`'s `test_migrate_from_mdbx_*` and
`env::layout::a_datadir_without_the_marker_is_v1_and_gets_it`. The one run on real `v0.16.0-adiri` datadirs is the manual e2e run above: the restarted validator
migrated both databases and recorded the marker, then could not peer. No lane, and no make target,
upgrades a whole committee from `v0.16.0-adiri` (every node stopped, then started together on this
release); rehearse that by hand before upgrading testnet.

**Back up first.** With the node stopped, copy the whole datadir (at least `consensus-db/`, `db/`
and `static_files/`) before the first start of the new binary. Both migrations are one-way, and
the backup is the only way back to `v0.16.0-adiri` (see Rollback).

**Rebuilding in the new layout.** `telcoin-network db reset-execution` moves the execution database
aside, so the next start rebuilds it from genesis by re-executing the chain, in the layout new
datadirs get. It moves `db/`, `static_files/` and `restored-state-floor` (the floor of a datadir
restored from a snapshot, which would refuse the rebuilt chain's old blocks if it stayed) into
`<datadir>/execution-reset-<UTC time>/`. It renames and never deletes; the consensus database, keys
and config stay, and so does reth's transaction-pool blob store (`blobstore/`), which holds pool
state, not chain data. It refuses while a node holds the datadir, and is a dry run without `--force`. The
moves are staged in `execution-reset-<UTC time>.partial/`, renamed once every entry is in it; a node
refuses to start while a `.partial` directory is there, and running the command again finishes it.
It refuses, and moves nothing, a datadir whose consensus database holds a later epoch's pack and none
for epoch 0, as a datadir bootstrapped with `db load-state` does (it holds consensus only from the
restored epoch on). The rebuild replays epoch 0 from the node's own packs, and the node fetches no
pack below the epoch its consensus database is at, so it could not start. Such a node rebuilds as a
new one: move `consensus-db/` aside as well, as for a forked record below, and it syncs from genesis.

- **In place** (an observer, or a validator that can be out for the rebuild):

  ```sh
  # node stopped
  telcoin-network db reset-execution --datadir <datadir>          # dry run: prints what moves
  telcoin-network db reset-execution --datadir <datadir> --force
  telcoin-network node --datadir <datadir> ...                     # rebuilds from genesis
  ```

  The node re-executes the consensus output its own consensus database holds, epoch by epoch from
  epoch 0, then the output after it from peers. Each epoch it re-executes is checked against the
  epoch record it already holds (certified, or its own from before the reset): when it closes the
  epoch, and again when it enters the next one, the record's `final_state` must be the epoch's
  closing block in the rebuilt chain, number and hash.
  Block hashes chain, so this covers every block up to that closing block: the check finds the
  first epoch that differs, not the block. On a mismatch the node logs `stored epoch record
  final_state does not match this node's closing block` with the epoch and both blocks, counts
  `tn_epoch_final_state_mismatches_total`, and stops; a restart stops again when it enters the next
  epoch. Only the epoch-0 placeholder is not compared: the epoch-0 close replaces it. A
  `restored-state-floor` beside a full consensus database (one copied in with a restored
  observer's execution database by the swap below) moves aside with the execution database, so the
  rebuild executes and compares every epoch, the restored one included (the restored-block
  exemption under "Snapshot export / restore" does not apply here). It is out of the
  committee's work for the whole rebuild. On a five-validator committee with a quorum of four, one
  validator out leaves no margin, so validators take the swap below.

  A validator rebuilt in place keeps the votes and proposals it made in the epoch it was in: they
  are in `consensus-db/`, which the reset leaves, and the rebuild's closes of the epochs before
  it keep them (a close removes only the votes and proposals of the epoch it closes and older
  ones; each close that keeps some logs `keeping that epoch's votes and proposals` with the
  epoch). When the node is back in that epoch they keep it from voting twice in a round or
  proposing a second header for one, as after a restart, so it needs no wait before its start,
  unlike a fresh datadir (Rollback below).

  A record of the node's own that a bad execution wrote stops every rebuild on this release. A
  node that forked (its execution diverged, and it built and stored its own record for that
  epoch, which no quorum certified) still holds that record after the reset. The rebuild, on a
  fixed binary, stops at that epoch with an `uncertified` mismatch, and again on every restart.
  Nothing replaces the record in place: the consensus database keeps one record per epoch, refuses
  a different one (a peer's certified record too), and no `db` command removes or rewrites one.
  The recovery is a fresh datadir, which keeps the keys and config:

  ```sh
  # node stopped
  telcoin-network db reset-execution --datadir <datadir> --force   # db/, static_files/, floor
  mv <datadir>/consensus-db <datadir>/consensus-db.forked           # the stale record's store
  telcoin-network node --datadir <datadir> ...                       # syncs from genesis
  ```

  The node then syncs like a new one: the certified records from peers, and the chain from
  genesis (or restore it from a state pack with `db load-state`, which needs this fresh datadir).
  A validator's votes and proposals for the epoch it was in stay in the moved-aside
  `consensus-db.forked/`, so a validator stays stopped until the network has closed the last
  epoch it took part in, as for a fresh datadir under Rollback below. The side-observer swap below
  is no way around this: it keeps the validator's `consensus-db/`, and the stale record with it.
  Keep the moved-aside directories until the cause is known: they hold the diverged chain.
- **Side-observer swap** (validators): build the new execution database on another node, then swap
  it in, so the validator is down only for the swap.
  1. Run an observer on the new binary with the validator's chain config in a fresh datadir (another
     host, or another datadir and ports on the same host), and let it sync from genesis to the tip.
     It gets the layout new datadirs get.
  2. Stop the observer cleanly at the tip, so its files do not change while they are copied.
  3. Stop the validator, then move its execution database aside:
     `telcoin-network db reset-execution --datadir <validator-datadir> --force`.
  4. Copy the observer's `db/` and `static_files/` (and its `restored-state-floor`, only if it has
     one) into the validator's datadir. Never copy its `consensus-db/` or its keys.
  5. Start the validator. It opens the copied execution database and executes the consensus output
     its own consensus database holds past the copy's tip, then rejoins.

**Rollback.**

- **To the database before a reset or a swap** (same binary): stop the node, move the new `db/`,
  `static_files/` and any `restored-state-floor` out of the datadir, move the entries of
  `execution-reset-<UTC time>/` back into it, and start. The node resumes from its old tip. Once the
  rebuilt node is healthy and no rollback is wanted, delete `execution-reset-*` to free the space.
- **To `v0.16.0-adiri`:** restore the backup taken before the upgrade, then start the old binary.
  There is no automatic rollback, and without a backup no safe way back for a validator. The old
  binary cannot open a tn-lmdb `db/` and has no backend guard; what stops it is the version file:
  it refuses `db/database.version` (1002, the tn-lmdb version) before it opens anything ("breaking
  database change detected: your database version (v1002) is incompatible with the latest
  database version (v2)"), so `db/` and `static_files/` are left as they were and the new binary
  still runs on them. Do not edit or delete the file to get past it: the old binary would then
  create an empty MDBX database beside `tn-lmdb.dat`, prune the static files down to it, and fail
  at genesis. The old binary opens the reth database before the consensus database, so it stops
  before it reaches `consensus-db/` either, which the new binary moved to tndb (one-way, #1558). On
  a fresh datadir the old binary starts with an empty consensus database and forgets the votes this
  validator cast in the current epoch, so it could equivocate: keep such a validator stopped until
  the network has closed the epoch it was in, then start the old binary on a fresh datadir. The
  version file is the default (tn-lmdb) build's tripwire. A build with `tn-reth`'s `mdbx` feature
  (no release is one) keeps the reth database on MDBX and `database.version` at reth's 2, but
  still moves the consensus database to tndb, so `v0.16.0-adiri` started on a datadir that build
  ran opens it without a refusal, with empty consensus databases: never run the `mdbx` build on a
  validator's datadir you may roll back.

### Persist benchmark

`env::persist_bench::per_block_persist` (`src/env/persist_bench.rs`, `#[ignore]`d) times
`RethEnv::persist_executed_output` alone on a production-configured chain: blocks of transfers (to
new and existing accounts) and contract writes (new and existing slots) over a prefilled state,
persisted one block per commit and several per commit (production persists once per consensus
output). It also reports reth's own split of each persist (`save_blocks_mdbx`, the database's
writes, and `save_blocks_commit_mdbx`, its commit, against the static files' write and commit; reth
keeps these names whatever the engine), so the engine's share is visible. The knobs (`TN_PERSIST_BENCH_*`) are in its doc comment;
`TN_PERSIST_BENCH_LAYOUT=v1` or `v2` compares the storage layouts on one build. Run each build from
its own target directory:

```sh
mkdir -p target/persist-bench-tmp
TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-mdbx \
  cargo test --release -p tn-reth --features mdbx --lib env::persist_bench::per_block_persist -- --ignored --exact --nocapture
TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-tn-lmdb \
  cargo test --release -p tn-reth --lib env::persist_bench::per_block_persist -- --ignored --exact --nocapture
```

Mac results, 2026-10-27 (release, default knobs: 400 blocks of 200 transactions over 10000
accounts; the median of 5 alternating runs per build; ms per commit):

| sync | blocks per commit | MDBX p99 | tn-lmdb p99 | ratio | database part p99, MDBX / tn-lmdb |
|---|---|---|---|---|---|
| no-sync | 1 | 78.3 | 72.2 | 0.92 | 43.8 / 14.4 (0.33) |
| no-sync | 4 | 86.9 | 77.7 | 0.89 | 41.9 / 30.6 (0.73) |

Both are well inside the reference (tn-lmdb's per-block persist p99 at most 1.25× MDBX's). Most of a
persist on macOS is reth's static files: their commit was then a `File::sync_all`, which is
`F_FULLFSYNC` on macOS, on every commit in both builds (a plain `fsync` since vendor/reth
TN-0117). The verdict uses the no-sync runs
(MDBX on `--db.sync-mode safe-no-sync`, tn-lmdb on the engine's `NoSync`). The durable runs
(3+3: p99 ratio 0.93 and 0.91) predate tn-lmdb's `msync` of mapped pages on macOS. Its scattered
commits then took a plain `fsync`, which leaves some of those pages unwritten there, so they priced
less work than MDBX's `msync` of its used span. Neither engine's durable commit flushes the drive's
cache on macOS. The backends' Linux comparison is the execution benchmark's
("Current result" below).

Under no-sync, `tn-lmdb.dat` grew to 2 GiB in every run, against 65 MiB durable: the pages a commit
frees are reused only after a sync, and a no-sync run never syncs. The engine then remapped past
its 1 GiB reservation, and those commits are the run's slowest (up to 436 ms, below p99). The
tn-lmdb build's `--db.sync-mode safe-no-sync` therefore adds automatic syncs (each second or 64
MiB, `tn_lmdb_args`), which bound that growth; the bench's no-sync runs still reach the engine's
`NoSync` with no syncs at all through a test-only path (`database::TEST_NO_SYNC`, which the bench
sets before it opens the database), so its rows time no sync.

### Execution benchmark

`etc/reth-exec-bench.sh <bench dir> [before-commit]` runs `tn-engine`'s busy-node execution bench
(`exec_bench` in `crates/engine/tests/exec_bench/main.rs`, `#[ignore]`d, its knobs in the module
docs), which times `execute_consensus_output` per output and splits each output's time by stage.
Without a commit it builds this checkout twice, MDBX (`--features tn-reth/mdbx`) and tn-lmdb
(the default features), each into its own target directory under the bench directory, and
refuses identical binaries; with a commit it compares that commit (which must have the bench)
against this checkout on one backend (`EXEC_BACKEND`, default `tn-lmdb`), each side built with
the flags its own manifest needs for it (a commit from before 2026-11-09 selects tn-lmdb with a
`tn-lmdb` feature). `etc/reth-exec-bench.sh --layout <bench dir>` builds this checkout once on
tn-lmdb (storage v2 is tn-lmdb only) and runs that one binary on storage layout v1 (A,
`TN_EXEC_BENCH_LAYOUT=v1`) against v2 (B, `TN_EXEC_BENCH_LAYOUT=v2`); its bases, one per layout,
share `cache/` with backend mode's. Each layout-mode run also records the bench's differential
(`TN_EXEC_BENCH_DIFF`, to `reports/<run>.diff`), and each round's second run compares its own to
the first's (`TN_EXEC_BENCH_DIFF_AGAINST`; with the alternating order, v2's in odd rounds and
v1's in even ones): the report lists each comparison and their total mismatches above the
profile's table, and a mismatch fails the run. In commit mode `EXEC_LAYOUT_BEFORE` and
`EXEC_LAYOUT_AFTER` (`v1` or `v2`) set one side's layout, and the before commit's bench may write
report version 5, from before the layout knob: such a bench refuses the knob, so for it set only
`EXEC_LAYOUT_AFTER`. It runs the profiles in
`EXEC_PROFILES` (`busy`, `busy-readers`, `peak`, `busy-metrics` by default; `busy-epoch`,
`busy-export` and `busy-history` when named) as interleaved pairs
(`EXEC_ROUNDS`, default 2: A B B A), passes any other `TN_EXEC_BENCH_*` knob through, and writes one
report to the repo root, `reth-exec-<host>-<time>.txt`: per profile, each side's medians over the
rounds and each pair's B/A ratio, with the median ratio and its spread, beside each run's JSON
report under `<bench dir>/reports/`. `EXEC_PAGE_SIZE_MDBX` and `EXEC_PAGE_SIZE_TN_LMDB` set each
backend's database page size independently (the bench's `TN_EXEC_BENCH_PAGE_SIZE`, a power of two
from 4096 to 65536 bytes; unset, the backend's default from `default_page_size`: MDBX the OS page,
tn-lmdb 4096), and the report names each side's page size; each page size has its own cached bases.
The script's header lists its settings and everything it writes. On macOS both backends' durable
commits are `msync` (libmdbx its used span, tn-lmdb its dirty pages), and neither flushes the
drive's cache. The bench's `TN_EXEC_BENCH_LAYOUT` (`v1`, or `v2` on tn-lmdb only,
through `StorageLayout::select`) picks reth's storage layout, recorded in each report and checked against
every opened datadir; `TN_EXEC_BENCH_READ_MIX=history` (the `busy-history` profile) has the readers
read balances and storage at past blocks and transactions by hash; and `TN_EXEC_BENCH_DIFF` writes
a seeded sample of past-block and by-hash reads with their answers, which `TN_EXEC_BENCH_DIFF_AGAINST`
compares to an earlier run's (two layouts or backends of one load must answer alike). The report
also splits the static files by segment, and has a section for their per-segment sync timing that
stays empty until the static-file provider times them. The stages named `mdbx`
(`mdbx` and `commit mdbx`, the JSON keys `save_blocks_mdbx` and `commit_mdbx`) carry reth's metric
names, which reth keeps whatever the engine: on tn-lmdb they time tn-lmdb.

From the per-output rows the script's report adds the tail and drift rows: the total's p90, p95 and
p99 pooled over every output of a side's paired rounds (one percentile, linear between the closest
ranks), the database commit's last-third over first-third mean per run, and one line per run of the
commit's mean over ten consecutive groups of outputs. It then adds a block per side of the
database writes per output (the commit's ms, disk MiB, file growth and each counter that moved),
each as the run's mean, first third and last third, ending with the commit's ms per page written
(tn-lmdb: per `tn_lmdb_pages_synced`; MDBX: per `mdbx_newly + mdbx_cow + mdbx_split`): a commit that
grows at the same ms per page writes more pages, one that grows per page writes the same pages
slower. Each profile also prints each side's chain, the genesis, aged-tip and final-tip hashes in
its runs' JSON headers: execution is deterministic, so in backend and layout modes, and in commit
mode with `EXEC_SHARED_BASE=1` (both sides on the before side's aged bases), a difference is marked
`CHAINS DIFFER` and fails the script after the report. `etc/reth-exec-bench.sh --report-only <bench
dir>` renders the report again from a bench directory's `reports/runs.tsv`, with no builds and no
runs. The script reads JSON report versions 6 (the bench's `JSON_REPORT_VERSION`) and 5 (6 only
added fields, which a version 5 run renders as `-`): it refuses a directory without `runs.tsv` or
with a report of another version, and refuses before building this checkout's bench when it
writes another than 6, and commit mode's before bench when it writes neither. On Linux,
`EXEC_SYS_SAMPLE=1` runs a sampler beside each run (every second: `/proc/meminfo`'s Dirty and
Writeback, the `/proc/diskstats` line of the whole disk under the bench directory and the drive's
temperature; `filefrag`'s extent count of the data file when it appears and every 30 s, when
installed), and the report gives per side the disk's write MiB/s, ms per write, busy %, flushes and
ms per flush, Dirty and Writeback, the temperature and the extents over each run's timed loop (its
end placed by the line the bench prints and flushes the moment the loop ends) and its first and last
third. It samples the whole disk, so other writers on it show up.

Each timed output also records what it wrote, read just before and just after its
`execute_consensus_output` call (outside its wall time; the database commit completes inside the
call on both backends): the database's own counters over the call (`db_counters`, `mdbx_*` or
`tn_lmdb_*`, see `src/test_utils/load/db_counters.rs`), the bytes the process wrote to disk
(`proc_pid_rusage`'s `ri_diskio_byteswritten` on macOS, `/proc/self/io`'s `write_bytes` on Linux,
none elsewhere) and the database data file's length after it. The report's "database writes per
output" section gives each counter that moved as its mean per output (`_ns` counters in ms, the
`tn_lmdb_max_gate_lag` maximum as its largest), the disk MiB and the data file's growth per output and
over the run, then deciles (ten consecutive groups of outputs) of the commit's ms, the disk MiB and
the file's length, which show whether the commit grows over a long run. The JSON report carries
them per row (`db_counters`, `disk_bytes_written`, `db_file_len_bytes`), the source and
the counters' names in the build's order in its header (`disk_bytes_source`, `db_counter_names`)
and the summary as `db_writes`. Caveats: the disk bytes are an
approximation: Linux counts a page as it is dirtied (a page dirtied once and synced counts once),
including the static files' writes and anything else the process writes, and macOS counts the I/O
the process issues and misses background writeback; a "pages synced" counter is in database pages,
not bytes (an OS page larger than the database page writes more); MDBX's counters count from its
lock file's creation, per environment, so only the per-output differences mean anything.

Each run opens a copy of a cached base datadir (`clone_datadir`). On Linux the copy is then evicted
from the page cache (each regular file synced, then `posix_fadvise(POSIX_FADV_DONTNEED)`), so both
backends start cold, as a restarted node does. Without it `cp` leaves the copy cached in large
page-cache folios, and a page a commit dirties through a shared mapping writes back its whole folio
(measured on Linux 7.1, ext4: 439 KiB written per 4 KiB page dirtied in a file written by `write(2)`
and still cached, 4 KiB in one read cold at random): in a Linux run on a cached copy, tn-lmdb's busy
commit grew from 82 to 583 ms over 300 outputs, all in its one `fdatasync` (199 to 532 MiB written
per commit), and MDBX wrote about 970 MiB per commit. Linux commit numbers from runs on a cached copy
are not comparable with later ones. On macOS nothing is evicted: it has no per-file eviction, and no
folio writeback of this kind. Where the file system clones (APFS always; XFS and btrfs with
reflink), the copy shares every block with the base, so a commit's first write to each block also
pays the file system's copy-on-write (a new block and an extent update), which a node's own file
never pays: Mac timings include it, on both backends, in proportion to each one's writes. ext4 has
no reflink: `cp` copies in full there. On Linux tn-lmdb also advises its mappings `MADV_RANDOM`
(`EnvOptions::readahead`, off by default there), as reth's MDBX does, so its own faults do not build
large folios.

#### Current result: Linux, 2026-11-04

Backend mode on a Linux machine (a consumer NVMe drive, ext4) at commit 06a413e9e: the six
profiles, 2 ABBA rounds each, 300 timed outputs per run, durable sync, the bench's defaults
otherwise. tn-lmdb passed on all six:

| profile | tn-lmdb / MDBX | bar |
|---|---|---|
| `busy` | 0.980 | 1.02 |
| `peak` (20 batches of 30M gas) | 1.013 | 1.02 |
| `busy-readers` (4 readers, the poller, the submitter) | 1.009 | 1.05 |
| `busy-metrics` (Prometheus, with readers) | 1.033 | 1.05 |
| `busy-epoch` (a close every 50 outputs) | 1.008 | 1.05 |
| `busy-export` (a state-pack export beside the writer) | 1.006 | 1.05 |

The drive syncs at one of two speeds and flips between them mid-run for either engine, so a whole
run's ratio follows the drive's state (0.79 to 1.11 in this run) and is not judged. Each ratio above
is over the pairs of outputs, one per backend, whose commits both ran in the fast state (under 4 µs
per page written; 99 to 308 pairs per profile).

`peak`'s remaining gap on those pairs, about 16 ms of a 173 ms commit, is the page CRC stamp
(about 15 ms an output): tn-lmdb's data sync costs what MDBX's whole commit costs for the same pages
(156 ms for 98k pages against 157 ms for 100k), and MDBX has no page checksums. The stamp's cost is
the open item; the checksums stay.

Baseline from an earlier engine: a Mac run of 2026-10-28 (Apple M1 Max, APFS, so on block-sharing
clones) had tn-lmdb's mean output time at 1.00 to 1.08x MDBX's on the busy profiles and 1.48x on
`peak`.

#### The load generator (`src/test_utils/load/`, `test-utils`)

- **`CaptureRecorder`:** a global metrics recorder that keeps the raw samples of allowlisted
  histograms (raw names such as `storage.providers.database.save_blocks_total` and
  `tn_reth.block_execute_seconds`) and forwards the rest to an optional inner recorder (tn-metrics'
  `build_recorder`). Install it before the first `RethEnv::new_database`: reth's metric handles bind
  to the recorder current at their first registration.
- **`env`:**
  - `open_production_env` opens a reth env as the node opens it: the given `--db.*` flags parsed by
    `RethCommand`, never the temp chain's caps; HTTP RPC on, IPC off, ports 0.
    `open_production_env_with_db` also returns the database handle (`RethDb`), which keeps the
    database open (and, on tn-lmdb, locked) until it drops too.
  - `clone_datadir` copies a closed datadir with `cp -c -R` (APFS `clonefile`) on macOS and
    `cp -a --reflink=auto --sparse=always` elsewhere, and refuses an existing destination. On
    Linux it then evicts the copy from the page cache (`evict_from_page_cache`: every regular file
    synced, then `posix_fadvise(POSIX_FADV_DONTNEED)`; a failure is an error naming the file), so
    the env opened on it starts cold; elsewhere that step does nothing.
  - `announce_temp_dir` prints a bench's temp dir and stops on tmpfs/ramfs (`fs_type`, `fs_type_in`
    over Linux's mountinfo; the persist bench uses it too).
  - `db_file_bytes` and `dir_bytes` give the length and allocated bytes of the database file and of
    a directory such as `static_files`.
- **`db_counters`:** backend-neutral reads of the open database. `db_counters(&db)` gives
  `DbCounters`, its cumulative counters by name: MDBX's page operations (`Info::page_ops`, as
  `mdbx_newly`, `mdbx_cow`, `mdbx_msync`, ...) or tn-lmdb's `IoStats` (as `tn_lmdb_commits`,
  `tn_lmdb_pages_synced`, ..., times in `_ns`); `since` subtracts an earlier read, so a read before
  and after an output gives what it wrote. A read is one `mdbx_env_info_ex` or one short lock of
  tn-lmdb's published copy. `db_page_size(&db)` gives the page size the file was created with.
- **`state`:** `PrefillSpec { accounts, senders, pools, seed, registry }` builds a deterministic
  prefilled genesis (`genesis()`), on `test_genesis_at(GENESIS_TIMESTAMP)` or, with `registry`, on
  `test_genesis_with_consensus_registry` (which needs a tokio runtime).
  - `senders` keyed accounts (`sender(i)`, `sender_factories()`: keys from a `StdRng` seeded with
    `keccak256("sender" ‖ seed ‖ i)`) and `accounts` keyless addresses (`account(i)`) hold native
    balances and balances on two `Stablecoin` tokens, `STABLECOIN_A` and `STABLECOIN_B`. The tokens
    are the artifact's `deployedBytecode` (it has no immutables), with OpenZeppelin 5.2's ERC-7201
    `ERC20Storage` written directly: `balance_slot`, `allowance_slot`, `total_supply_slot`.
  - `SWAP_ROUTER` is hand-assembled code (`router_code`) taking calldata `(pool, amount_in)`
    (`swap_calldata`). It calls `A.transferFrom(caller, router, amount_in)` on each sender's
    prefilled allowance, rewrites the pool's two reserve slots (`reserve_slots`), pays
    `B.transfer(caller, amount_out)` from its prefilled balance and logs `ISwapRouter::Swap`. It
    reverts when a token call fails.
  - On the test chain a token transfer uses 38,974 gas and a swap 79,520.
- **`base`:** `build_base(spec, page_size, cache_dir)` builds a spec's genesis once per backend and
  page size into a closed datadir, `<cache_dir>/base-<backend>-pg<page_size>-<key>`, with
  production `--db.*`, durable sync and `--db.page-size <page_size>`; a database with another page
  size (`db_page_size`) is an error.
  - The key (`base_dir_name`) is 16 hex digits of `keccak256` over the spec and `LAYOUT_VERSION`. A
    derived datadir, such as an aged one, appends its own `extra` component. A change that makes a
    base built by earlier code differ from what the code builds bumps `LAYOUT_VERSION`, which
    renames every base: each backend's bases, and their aged children, rebuild once. The constant's
    doc comment lists each version and why.
  - It writes `bench-base.json` (`BaseMeta`: the spec, the backend, the page size, the layout
    version, the sealed genesis header, the slim genesis hash, build times and sizes), then renames
    `<name>.building` into place. A later call returns the cached base.
  - An existing base whose meta does not match (layout version, backend, page size, spec, header
    hash, or the slim genesis hash, which changes with the token or router code or the test
    genesis), or that has no meta, is an error. It is never rebuilt over.
  - `slim_chain(meta)` reopens a clone of the base: the chain spec of `PrefillSpec::slim()` (no
    senders or accounts) with the cached genesis header in place of its own, so an open never
    recomputes a 1M-account genesis root. `init_genesis` checks an existing genesis by hash only and
    refuses another header (`GenesisHashMismatch`). The slim chain's `genesis().alloc` lacks the
    prefill; nothing on the open or execute path reads it.
  - Measured 2026-10-27 (Apple M-series laptop, release, durable, `build_base_at_scale` with 1M
    accounts and 10k senders), at layout version 1, before the engine's dup tables; the tn-lmdb
    file has changed since:

    | backend | build | genesis and chain spec | `init_genesis` | data file allocated | peak RSS |
    |---|---|---|---|---|---|
    | MDBX | 12.9 s | 4.2 s | 8.6 s | `mdbx.dat` 712 MiB | 4.4 GB |
    | tn-lmdb | 13.2 s | 4.2 s | 9.0 s | `tn-lmdb.dat` 1040 MiB | 4.7 GB |
- **`workload`:** `Workload::new(env, spec, WorkloadConfig)` draws a seeded mix from the senders'
  on-chain nonces: by default 40% native transfers, 40% ERC20 transfers, 15% swaps and 5% 1 KiB
  creations, with 20% fresh recipients and the rest Zipf-hot `account(i)`. `next_batch(target_gas)`
  signs a `LoadBatch` filled by exact expected gas, each gas limit 10% over it and within
  `block_gas_limit` (30M). A transaction that fits no batch is refused, not left to empty every
  later one: `Workload::new` refuses a mix whose largest transaction (`largest()`, an upper bound)
  exceeds `block_gas_limit`, `next_batch` panics on a target below it, and `OutputFactory::new`
  refuses a `batch_gas` below it. Gas on the test chain:

  | transaction | gas |
  |---|---|
  | native transfer | 21,000 |
  | ERC20 transfer to a holder | 21,000 + calldata + 17,390 (~39.0k) |
  | ERC20 transfer to a fresh address | 21,000 + calldata + 34,490 (~56.1k) |
  | swap | 21,000 + calldata + 58,180 (~79.5k) |
  | 1 KiB creation | ~275k |
- **`output`:** `OutputFactory::new(env, spec, WorkloadConfig, OutputConfig)` packs `batches` (8)
  workload batches of about `batch_gas` (15M) into each production-shaped `ConsensusOutput`
  (`next_output`, or `generate(n)` up front).
  - On a `registry` spec the leader rotates through the registry's committee, which `committee()`
    returns for `set_committee`.
  - Outputs chain after the output the env's tip executed (its `parent_beacon_block_root` and
    nonce, with the leader round twice the consensus number). They are committed `interval_ms`
    (1 s) apart from one second past the tip, so `execute_consensus_output` never clamps a block's
    timestamp.
  - On a `registry` spec `next_output_closing()` builds an output that closes its epoch. Once it
    has executed, `begin_epoch(env)` enters the next epoch: its number and committee are read from
    the registry at the tip, and the seed chain restarts at the epoch root. A factory opened on a
    tip that closed an epoch starts there too. `generate` never crosses a pending close.
- **`readers`:** the concurrent side load of a busy node.
  - `Readers`: reader threads call the in-process HTTP RPC methods through `Handle::block_on` on a
    multi-thread runtime. They call `eth_getBalance` and `eth_call` of `balanceOf` for Zipf-drawn
    accounts, `eth_getTransactionReceipt` of recent transactions, `eth_getLogs` of the router's swaps
    over 64 blocks, and `eth_getBlockByNumber(latest, true)`. A thread also runs the batch builder's
    `get_account_balances` poll every 250 ms. All of them sit behind one on/off gate, whose close
    waits for the calls in flight.
  - `Submitter`: `eth_sendRawTransaction`, counting `nonce too low` and `already known` apart from
    errors.
  - Each drains per-window latency samples and error counts, keeping the first error message.

## Determinism rules

Block production must be a pure function of certified consensus output. Concretely:

- No wall-clock reads in state-affecting paths; block timestamps come from the consensus commit
  timestamp carried in the payload.
- No hash-map iteration in state-affecting paths — rewards use `BTreeMap`, committees are sorted
  by address before encoding (`generate_conclude_epoch_calldata`).
- Epoch-close randomness is epoch-gated (`tn-types` `forks::seed_signature_active`): before the
  fork epoch it is the legacy keccak256 of the leader certificate's aggregate BLS signature,
  wire-identical to pre-fork releases; from the fork epoch on it is the epoch seed chain value as
  of the closing commit, folded per commit in consensus (`tn-types` `primary/output.rs`). Either
  way it is carried in the payload, seeds the committee-shuffle RNG **and** is stored in
  `extra_data`, so production (`context_for_next_block`) and replay (`context_for_block`) derive
  identical state.
- The shuffle's RNG draw order is consensus-critical and pinned by a golden-value unit test in
  `src/evm/block.rs`; the fee penalty uses platform-independent u128 integer math.
- Failure handling is deterministic too: unrecoverable transactions are dropped identically on
  every node from the same certified bytes, and invalid transactions are skipped, never
  reordered.

## Module map

| Module | Owns |
|---|---|
| `src/cli.rs` | `RethCommand`/`RethConfig`: RPC namespace allowlist, disabling reth networking/discovery, per-sender pool-slot default (256). |
| `src/dirs.rs` | TN data-directory layout and conversion to reth `DatadirArgs`. |
| `src/env/mod.rs` | `RethEnv` wrapper (node config, blockchain provider, EVM config, task spawner); DB open/genesis init; sets `BASEFEE_ADDRESS`; shared read-only state-DB stack. |
| `src/env/database.rs` | The database backend (`RethDbEnv`/`RethDb`, MDBX or tn-lmdb): `open_rw`, the backend guard, the `--db.*` mapping, the table stats of `db stats` (`db_table_stats`), `db load-state`'s refusal of a pending migration (`refuse_pending_migration`); see "Database backend". `src/env/database/migrate.rs`: the MDBX to tn-lmdb migration (tn-lmdb build). |
| `src/env/epoch.rs` | Startup healing and epoch-boundary reads: `EpochState`, committees, BLS pubkeys, worker fee configs, classified registry reads (node-local vs chain-global failures). |
| `src/env/exec_cache.rs` | The cross-block execution cache (adapted from reth's `cached_state.rs`): account, slot and bytecode values at the last built block's hash, used by the next block built on it and refreshed from each block's bundle; dropped on any failure. Its module docs hold the invariant and the state writers that must not bypass it. |
| `src/env/execution.rs` | `build_block_from_batch_payload` (recover → execute → assemble), `finish_executing_output` (atomic blocks + finality markers), `finalize_block` (in-memory watches). |
| `src/env/format_freeze_tests.rs` | Test-only: the format-freeze fixtures' generator and tests (see "On-disk format versions" under "Database backend"). |
| `src/env/genesis.rs` | Genesis construction, including `ConsensusRegistry` account generation via a pre-genesis EVM and embedded-artifact JSON parsing; temp-chain env for tests. |
| `src/env/helpers.rs` | Read-side accessors: headers, blocks, receipts, accounts, `read_contract*`, canonical streams and tips. |
| `src/env/persist_bench.rs` | Test-only, `#[ignore]`d: the per-block persist benchmark (`per_block_persist`; see "Persist benchmark" under "Database backend"). |
| `src/env/rpc.rs` | Assembles and starts reth's RPC server over the worker pool + `WorkerNetwork`; no engine namespace. |
| `src/error.rs` | `TnRethError` and the state-read error taxonomy. |
| `src/evm/mod.rs` | `TNEvm` wrapper over revm; `transact_system_call` (100M gas, fee-exempt, nonce-check disabled); pre-genesis create. |
| `src/evm/block.rs` | `TNBlockExecutor` (pre-block system contracts, epoch-close sequence, registry and governance-Safe forks) and `TNBlockAssembler` (header assembly); committee assembly. |
| `src/evm/config.rs` | `TnEvmConfig`: EVM env derivation, difficulty packing, `extra_data` decode for replay. |
| `src/evm/context.rs` | revm context type aliases and builder traits. |
| `src/evm/factory.rs` | `TNEvmFactory` / `TNBlockExecutorFactory`; installs TEL + BLS precompiles on every EVM instance. |
| `src/evm/handler.rs` | `TNEvmHandler`: base-fee crediting and the gas-limit penalty. |
| `src/evm/utils.rs` | `calculate_gas_penalty`. |
| `src/evm/tel_precompile/` | Native TEL issuance at `0x…07e1` (see its README). |
| `src/evm/bls_precompile/` | BLS12-381 signature verification at `0x…b151`. |
| `src/forward.rs` | `WorkerRpcForwarder`: observer → committee transaction forwarding. |
| `src/metrics.rs` | Block-building drop counters (`unrecoverable` alertable, `invalid` expected), the per-block stage timings, the epoch-close system-call gas gauges, and the `--forward-txs` submission counters. |
| `src/payload.rs` | `TNPayload` and `BuildArguments`: consensus data shaped for execution. |
| `src/rpc_server_args.rs` | RPC CLI argument subset and transport-limit defaults. |
| `src/rpc_tx_forward.rs` | `EthSubmitForwarded`: the `--forward-txs` handlers for `eth_sendRawTransaction` and `eth_sendRawTransactionSync` (local precheck, forward, local receipt wait for the sync method). |
| `src/rpc_tx_forward/target.rs` | `--forward-txs` parsing (`parse_forward_targets`), `ForwardTargets` (at most 8, deduplicated after normalization, redacted `Debug`) and `TxForwardConfig`. |
| `src/rpc_tx_forward/client.rs` | `TxForwarder`: one jsonrpsee `HttpClient` per target, ordered failover with demotion, the time, size and concurrency bounds, and the fixed unavailable error. |
| `src/snapshot.rs` | State-pack export (`PinnedStateView`) and verified restore (`SnapshotRestorer`). |
| `src/system_calls.rs` | `sol!` bindings for `ConsensusRegistry`/`WorkerConfigs`, `SYSTEM_ADDRESS`, registry address, `EpochState`. |
| `src/traits.rs` | `TelcoinNode` node-type wiring and the fail-loud `TNExecution` shim. |
| `src/txn_pool.rs` | `WorkerTxPool` wrapper: canonical-state maintenance, blob-tx removal, raw-tx recovery helpers. |
| `src/types.rs` | Type aliases (`RpcServer`, `RethDb`, `PoolTxn`, `TNPrimitives`); `RethDb` is re-exported from `src/env/database.rs`. |
| `src/worker.rs` | `WorkerComponents` and the `WorkerNetwork` RPC shim (libp2p peer count for `net_*`). |
| `src/test_utils.rs` | `TransactionFactory` and payload-execution helpers (`test-utils` feature / tests). |
| `src/test_utils/load/` | Shared code for the busy-node execution bench (`test-utils`): the capture recorder, the production env and datadir clones (`env`), the database's counters and page size (`db_counters`), the prefilled state (`state`) and its cached base (`base`), the transaction mix (`workload`), the consensus outputs (`output`), the RPC side load (`readers`), the storage-layout differential (`differential`), the storage layout choice (`layout`) and the static files' sync timing (`sf_sync`). See "Execution benchmark". |

## Feature flags

From `crates/tn-reth/Cargo.toml`:

| Feature | Effect |
|---|---|
| `faucet` | Instant role-gated TEL mint replacing the timelocked mint. **Never on mainnet.** |
| `adiri` | Adiri-testnet build: forwards `tn-types/adiri`, which compiles the adiri fork gates in `tn-types` `forks.rs`, and **implies `faucet`**. Each gate names the first adiri epoch on the new behaviour: 383 seed signatures (epoch seed chain replaces the leader-aggregate shuffle seed), 407 `ConsensusRegistry` and `WorkerConfigs` swap, 554 governance Safe onto the canonical v1.4.1 suite, 567 leader-seeded intra-round certificate order, 570 multi-worker `Committee` encoding, 574 seed-chain `PREVRANDAO`, 747 millisecond consensus timestamps. This crate applies the two state-swap forks (407 and 554) itself, in the closing block of the epoch before (steps 1 and 2 of "Epoch close"). Non-adiri builds compile none of these constants and run the post-fork behaviour from genesis. |
| `test-utils` | Test factories and payload helpers. |
| `js-tracer` | Serve JavaScript tracers in `debug_trace*` (geth's custom tracers). Off by default: it compiles boa, a JavaScript engine (35 packages). Without it a JS tracer request gets error `-32603` `JS Tracer is not enabled`, and the built-in tracers work as before. Forwarded by `tn-node` and the `telcoin-network` binary. See [js-tracer.md](js-tracer.md). |
| `mdbx` | Run the reth execution database on MDBX (`reth_db::DatabaseEnv`), the legacy backend, instead of the default tn-lmdb engine (`tn_lmdb_reth::TnLmdbEnv`). Off by default, and off in every release: kept for the tests of the legacy path until testnet has resynced. The default build's first open migrates an MDBX database; see "Database backend". |

The crate takes reth's individual crates, never the `reth` umbrella crate (which only re-exports
them and pulls reth's CLI, `reth-ethereum-cli` and `reth-cli-commands`, into the graph). Three of
the umbrella's default features are always on, enabled on the crates it forwarded them to:
`keccak-cache-global` (`reth-node-core`), `asm-keccak` (`reth-node-core`, `revm`) and
`min-debug-logs` (`reth-node-core`, which caps `tracing` at debug in release builds). `js-tracer`
is this crate's opt-in feature above. `jemalloc` is off because no TN binary installs jemalloc as
its global allocator, and `otlp`/`otlp-logs` because they only fed reth's own OTLP exporters,
which are removed (vendor/reth CHANGES.md TN-0029).

reth's node builder is not a dependency: this crate wires the provider, EVM, pool and RPC itself.
`revm` is a direct dependency only to keep the precompile backends (`secp256k1`, `blst`, `c-kzg`)
and `memory_limit` on. `reth-trie-sparse` is a direct dependency only for its `std` and `metrics`
features. reth's node crates used to enable all of these. See `vendor/reth/ROADMAP.md`.
