# tn-reth

`tn-reth` is the single boundary between Telcoin Network's BFT consensus (Narwhal/Bullshark) and
reth, which TN uses as an execution library. Every read or write of EVM state by the rest of the
node goes through this crate's `RethEnv` wrapper; the goal is that no other crate touches reth
internals directly (`src/lib.rs`).

This document is written for maintainers and security researchers. Everything below is derived
from the code at the cited paths; when the code and this file disagree, the code wins.

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

### Block-building timings

`RethEnv::build_block_from_batch_payload` times the stages of each block it builds into six
histograms (`RethEnvMetrics` in `src/metrics.rs`), one sample per block each. Execute, finish and
overlay extend run back to back, and the other three are parts of them. A stage that fails records
nothing, and neither do the stages after it.

| Metric | Spans |
|---|---|
| `tn_reth_block_execute_seconds` | Creating the block builder, the pre-execution changes, signer recovery and the transaction loop (`src/env/execution.rs`). |
| `tn_reth_block_signer_recovery_seconds` | Recovering every transaction's signer, in parallel on rayon. Part of execute. |
| `tn_reth_block_finish_seconds` | The block builder's `finish`: the post-execution changes (on an epoch-closing block, the closing system calls), merging the state transitions, hashing the post-state, the state root, and assembling the block. |
| `tn_reth_block_hashed_state_seconds` | Hashing the block's post-execution state (`hashed_post_state`). Part of finish. |
| `tn_reth_block_state_root_seconds` | The state root and trie updates over the output's accumulated overlay (`OutputTrieOverlay::layered_root_with_updates` in `src/env/output_overlay.rs`), from opening its read-only database transaction to closing it. Part of finish. |
| `tn_reth_block_overlay_extend_seconds` | From the end of finish to the built block: sorting the block's hashed state and trie updates, then extending the output's overlay with them. |

None of them covers the persist, which runs once per output after every block is built; reth's
`save_blocks_*` histograms time that. The Prometheus buckets for `_seconds` series (tn-metrics)
start at 5 ms, so a stage shorter than that reads only as "under 5 ms" on a dashboard.

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
  [`docs/gas-penalty.md`](../../docs/gas-penalty.md) at the repo root. Keep the two in sync.
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
transitively enables faucet minting.

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
  `eth_call` gas cap, a 1-ether (1 TEL) RPC transaction fee cap, tracing/filter/proof limits, and
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

## Database backend

TN's reth execution database lives in `<datadir>/db`. The default build runs it on MDBX
(`reth_db::DatabaseEnv`, `db/mdbx.dat`). The `tndb-b` feature (forwarded by `tn-node`,
`telcoin-network-cli` and `telcoin-network`) runs it on the tndb-b engine instead
(`tndb_b_reth::TndbEnv`, `db/tndb.dat`). reth is not patched: it is generic over the database on
every path TN uses, and MDBX stays compiled in both builds. Every backend difference lives in
`src/env/database.rs`; other code names `RethDbEnv` and `RethDb` (`Arc<RethDbEnv>`).

- **Open** (`open_rw`, called by `RethEnv::new_database`): the MDBX build opens through reth's
  `init_db` with `with_metrics`. The tndb build maps the `--db.*` flags, writes or checks reth's
  `database.version` file as reth's `create_db` does, opens `TndbEnv`, creates reth's tables and
  records the client version in `VersionHistory` as reth's `record_client_version` does.
- **Backend guard** (`check_backend`, both builds): the MDBX build refuses a `db/` holding
  `tndb.dat`, and the tndb build migrates one holding `mdbx.dat` (below). Opening the wrong engine
  would create an empty database beside the datadir's static files, and reth's
  `check_consistency` would then prune the static files back to the empty database's tip. The
  checks use `try_exists`, so a stat error fails the open. The MDBX build's error names the
  directory and both engines; the operator runs the matching build or starts from a fresh datadir
  (or `db load-state`).
- **MDBX to tndb-b migration** (`src/env/database/migrate.rs`, tndb build): the first open of a
  `db/` holding `mdbx.dat` moves it onto tndb-b ("Migrating from MDBX" below).
- **`--db.*` flags on tndb-b** (`tndb_args`): `--db.max-size` is the mapping's address-space
  reservation (unset: the engine's default, 1 GiB); past it the engine remaps at double the size,
  so it is not a cap. `--db.page-size` sets a new file's page size (the engine accepts a power of
  two in 4 KiB..=64 KiB). `--db.sync-mode durable` is durable commits. Refused at startup, naming
  the flag: `--db.exclusive`, `--db.read-transaction-timeout`, `--db.max-readers`,
  `--db.growth-step`, `--db.log-level`, `--db.sync-mode safe-no-sync`, and reth's `UtterlyNoSync`
  and `NoMetaSync` sync modes (neither has a CLI value). The no-sync refusals say why and point at
  `durable`: the engine's `NoSync` reuses the pages a commit frees only after a sync, and nothing
  syncs a `NoSync` environment on its own, so the file would grow until one (see "Persist
  benchmark"). They stay refused until the engine bounds that growth.
- **Default page size** (`default_page_size`): the page size of a new database without
  `--db.page-size`. The MDBX build's is reth's own default, the OS page size clamped to 4 KiB..=64
  KiB (16 KiB on Apple Silicon, 4 KiB on x86 Linux), which its `open_rw` passes to MDBX explicitly;
  the tndb build's is the adapter's 4 KiB. The execution bench keys its cached bases by it.
- **Table stats** (`db_table_stats`, behind `telcoin-network db stats`): each of reth's tables the
  database has, sorted by name, with its rows, branch, leaf and overflow pages, and size (the page
  size times its pages, `DbTableStat`). A table the database lacks is skipped. The MDBX build
  opens read-only through reth's `open_db_read_only`, which reads while the node runs, after
  refusing a `db/` holding `tndb.dat`. The tndb build opens `TndbEnv` read-only from the adapter's
  `table_stats`, which needs the node stopped (`DB_TABLE_STATS_NEEDS_STOPPED_NODE`): a database a
  node holds is refused with "in use: stop the node first", and `db stats` holds the datadir lock
  for the report. It never migrates: a `db/` holding `mdbx.dat` is refused (a migration pending
  or unfinished, which the node's start runs; beside the marker, as the open refuses it), and
  with neither `mdbx.dat` nor `tndb.dat` there is no reth database.
- **reth's dup-sort tables on tndb-b** (the adapter's crate docs, "Dup-sort tables"): the state
  tables (`PlainStorageState`, `HashedStorages`, `StoragesTrie`) are the engine's dup tables, each
  key's rows in a nested tree of their own, so a seek bounded to an account's or a trie's rows
  never reads the next key's; the changesets (`AccountChangeSets`, `StorageChangeSets`), appended
  by block and read by range, are composite keys (`key ‖ subkey`) in plain engine tables. A
  table's form is part of the file's format: the open refuses a file holding one in the other
  form (`MDBX_INCOMPATIBLE`, naming the table and both forms). No tndb-b file in the other form
  exists outside tests (the backend is pre-release and off by default), so there is no migration
  between the forms.
- **What differs on tndb-b:**
  - no per-operation latency metrics (MDBX's `with_metrics`). The sampled gauges keep MDBX's
    names (`db.table_size` and the rest), so dashboards and the e2e `reth_db_table_size` check
    hold;
  - the snapshot export's long read (`RethEnv::pin_state_view`) has no timeout to disable, and
    the adapter's long-reader warning can fire, at most once a minute, during a long export: the
    export holds back page reuse while it runs, so the file grows until it ends.

### Migrating from MDBX

A build with the `tndb-b` feature that finds an MDBX reth database (`db/mdbx.dat`) migrates it at
the node's first start, when `RethEnv::new_database` opens the database and before reth reads
anything. The module docs of `src/env/database/migrate.rs` have the states and steps. The node
logs `mdbx.dat`'s size, the live bytes of reth's tables in it and the free space, each table's
copy, and at the end the rows copied and the time taken. It warns of any table in MDBX that is not
one of reth's: its rows are not copied (reth does not read them either).

- **What it does:** it copies every one of reth's tables raw (encoded keys and compressed values;
  no row is decoded) into a new tndb-b file built in `db/tndb-migrating/`, with the node's
  `--db.page-size` and `--db.max-size`. Before anything of MDBX's is touched it checks each
  table's rows against MDBX's count, a read-back hash of each table against the copy, and the
  synced file against the engine's full check. Then it renames the file to `db/tndb.dat` (the
  commit point), removes `mdbx.lck` and then `mdbx.dat`, writes the marker `db/mdbx-migrated`
  (its text lists the rows copied per table) and removes the temp directory. reth's static files
  and `database.version` file are not touched. The copy needs no case of its own for the dup-sort
  tables' forms: MDBX walks a key's values in subkey order, so each row is an append of
  `key ‖ subkey` in order, a key's rows going to its nested tree on a dup table, also when they
  span the copy's commits (`a_dup_key_with_more_rows_than_a_commit_migrates_whole`).
- **Free space:** the filesystem holds both databases while it copies. The migration refuses to
  start unless the live bytes of reth's tables in MDBX plus a quarter plus 1 GiB are free
  (`needed_bytes`), and says the live bytes, how much is needed and how much is free. The live
  bytes are each table's branch, leaf and overflow pages times the page size, not `mdbx.dat`'s
  file size, which can be far more: MDBX grows the file in large steps (a local node's is 4 GiB,
  mostly sparse) and keeps the pages it frees. `db stats` on the MDBX build (the build being
  replaced) prints the same figures beside a running node, each table's "Total Size" and their sum
  in the "Tables" row, so the space can be checked before upgrading. The check is an early
  refusal: a disk that fills during the copy fails the migration the same way, leaving MDBX as it
  was.
- **Time:** in proportion to the database's size: one read of MDBX, one write and one read-back of
  the copy, and one sync. The on-demand test `migrates_a_copy_of_a_real_database` (in
  `src/env/database/migrate/tests.rs`; its doc comment has the command) migrates a copy of a real
  `db/` and checks every table, printing the live bytes and the free space needed. On 2026-10-27
  it migrated a small local-validator database (2110 rows; `mdbx.dat` a 4 GiB file whose tables
  take 704512 bytes of pages, so the migration needed 1074622464 bytes free, where the file's size
  would have asked for 6 GiB) in 45 ms on macOS, debug build, into a 1.0 MiB `tndb.dat`. A
  production-sized database has not been timed yet.
- **A stop part way** (a kill, a power loss) is safe at every step. Before the commit point MDBX is
  as it was, and the next start removes the temp directory and migrates again from the beginning.
  After it, the next start finishes the remaining steps: it removes what is left of MDBX's files
  and writes the marker.
- **Refusals.** The node does not start, and nothing is changed:

  | the node finds | why | what to do |
  |---|---|---|
  | less free space than the migration needs | the copy is built beside MDBX | free space, then start again |
  | a row tndb-b cannot store: a second value for a dup-sort subkey (`MDBX_KEYEXIST`), or a value over 16 MiB (`MDBX_BAD_VALSIZE`) | tndb-b stores one value per subkey and values up to 16 MiB; reth writes neither | MDBX is unchanged: run the MDBX build, and report the error |
  | a copy that fails its checks (a short read, a read-back mismatch, the engine's check) | the copy would lose or change rows | as above |
  | `mdbx.dat` beside `db/mdbx-migrated` | an MDBX build (an older binary) wrote MDBX after the migration, and the static files may be ahead of `tndb.dat` | resolve by hand: start from a fresh datadir, or `db load-state` |
  | `db/mdbx-migrated` without `tndb.dat` | the migrated database is missing | restore `tndb.dat`, or start from a fresh datadir (or `db load-state`) |

- **No rollback:** once `mdbx.dat` is removed there is no way back to MDBX. The MDBX build refuses a
  `db/` holding `tndb.dat`, so going back means a fresh datadir (or `db load-state`). Problems are
  fixed forward, as for the consensus DB.
- **`db stats`** never migrates: it refuses a `db/` holding `mdbx.dat` until a node start has run
  the migration.

### Persist benchmark

`env::persist_bench::per_block_persist` (`src/env/persist_bench.rs`, `#[ignore]`d) times
`RethEnv::persist_executed_output` alone on a production-configured chain: blocks of transfers (to
new and existing accounts) and contract writes (new and existing slots) over a prefilled state,
persisted one block per commit and several per commit (production persists once per consensus
output). It also reports reth's own split of each persist (`save_blocks_mdbx`, the database's
writes, and `save_blocks_commit_mdbx`, its commit, against the static files' write and commit), so
the engine's share is visible. The knobs (`TN_PERSIST_BENCH_*`) are in its doc comment. Run each
build from its own target directory:

```sh
TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-mdbx \
  cargo test --release -p tn-reth --lib env::persist_bench::per_block_persist -- --ignored --exact --nocapture
TMPDIR=$PWD/target/persist-bench-tmp CARGO_TARGET_DIR=target/persist-tndb \
  cargo test --release -p tn-reth --features tndb-b --lib env::persist_bench::per_block_persist -- --ignored --exact --nocapture
```

Mac results, 2026-10-27 (release, default knobs: 400 blocks of 200 transactions over 10000
accounts; the median of 5 alternating runs per build; ms per commit):

| sync | blocks per commit | MDBX p99 | tndb-b p99 | ratio | database part p99, MDBX / tndb-b |
|---|---|---|---|---|---|
| no-sync | 1 | 78.3 | 72.2 | 0.92 | 43.8 / 14.4 (0.33) |
| no-sync | 4 | 86.9 | 77.7 | 0.89 | 41.9 / 30.6 (0.73) |

Both are well inside the reference (tndb-b's per-block persist p99 at most 1.25× MDBX's). Most of a
persist on macOS is reth's static files: their commit is a `File::sync_all`, which is
`F_FULLFSYNC` on macOS, on every commit in both builds. The verdict uses the no-sync runs
(MDBX on `--db.sync-mode safe-no-sync`, tndb-b on the engine's `NoSync`), because a durable commit on macOS is `F_FULLFSYNC` for reth's
MDBX but a plain fsync for tndb-b, so durable rows are not a like-for-like engine comparison (3+3
durable runs: p99 ratio 0.93 and 0.91). Linux runs are pending.

Under no-sync, `tndb.dat` grew to 2 GiB in every run, against 65 MiB durable: the pages a commit
frees are reused only after a sync, and a no-sync run never syncs. The engine then remapped past
its 1 GiB reservation, and those commits are the run's slowest (up to 436 ms, below p99). The
tndb build therefore refuses `--db.sync-mode safe-no-sync` (`tndb_args`) until the engine bounds
that growth, and the bench's no-sync runs reach the engine's `NoSync` through a test-only path
instead (`database::TEST_NO_SYNC`, which the bench sets before it opens the database).

### Execution benchmark

`etc/reth-exec-bench.sh <bench dir> [before-commit]` runs `tn-engine`'s busy-node execution bench
(`exec_bench` in `crates/engine/tests/exec_bench/main.rs`, `#[ignore]`d, its knobs in the module
docs), which times `execute_consensus_output` per output and splits each output's time by stage.
Without a commit it builds this checkout twice, MDBX (the default features) and tndb-b
(`--features tn-reth/tndb-b`), each into its own target directory under the bench directory, and
refuses identical binaries; with a commit it compares that commit (which must have the bench)
against this checkout on one backend (`EXEC_BACKEND`, default `tndb-b`). It runs the profiles in
`EXEC_PROFILES` (`busy`, `busy-readers`, `peak`, `busy-metrics` by default; `busy-epoch` and
`busy-export` when named) as interleaved pairs
(`EXEC_ROUNDS`, default 2: A B B A), passes any other `TN_EXEC_BENCH_*` knob through, and writes one
report to the repo root, `reth-exec-<host>-<time>.txt`: per profile, each side's medians over the
rounds and each pair's B/A ratio, with the median ratio and its spread, beside each run's JSON
report under `<bench dir>/reports/`. `EXEC_PAGE_SIZE_MDBX` and `EXEC_PAGE_SIZE_TNDB` set each
backend's database page size independently (the bench's `TN_EXEC_BENCH_PAGE_SIZE`, a power of two
from 4096 to 65536 bytes; unset, the backend's default from `default_page_size`: MDBX the OS page,
tndb-b 4096), and the report names each side's page size; each page size has its own cached bases.
The script's header lists its settings and everything it writes. On macOS a durable commit is `F_FULLFSYNC` for libmdbx but a plain fsync for tndb-b, so
durable commit times there are not like-for-like between the backends.

From the per-output rows the script's report adds the tail and drift rows: the total's p90, p95 and
p99 pooled over every output of a side's paired rounds (one percentile, linear between the closest
ranks), the database commit's last-third over first-third mean per run, and one line per run of the
commit's mean over ten consecutive groups of outputs. With report v4 it adds a block per side of the
database writes per output (the commit's ms, disk MiB, file growth and each counter that moved),
each as the run's mean, first third and last third, ending with the commit's ms per page written
(tndb-b: per `tndb_pages_synced`; MDBX: per `mdbx_newly + mdbx_cow + mdbx_split`): a commit that
grows at the same ms per page writes more pages, one that grows per page writes the same pages
slower. `etc/reth-exec-bench.sh --report-only <bench dir>` renders the report again from a bench
directory's `reports/` (its `runs.tsv`, else the report names), with no builds and no runs. On Linux,
`EXEC_SYS_SAMPLE=1` runs a sampler beside each run (every second: `/proc/meminfo`'s Dirty and
Writeback, the `/proc/diskstats` line of the whole disk under the bench directory and the drive's
temperature; `filefrag`'s extent count of the data file when it appears and every 30 s, when
installed), and the report gives per side the disk's write MiB/s, ms per write, busy %, flushes and
ms per flush, Dirty and Writeback, the temperature and the extents over each run's timed loop and
its first and last third. It samples the whole disk, so other writers on it show up.

Each timed output also records what it wrote, read just before and just after its
`execute_consensus_output` call (outside its wall time; the database commit completes inside the
call on both backends): the database's own counters over the call (`db_counters`, `mdbx_*` or
`tndb_*`, see `src/test_utils/load/db_counters.rs`), the bytes the process wrote to disk
(`proc_pid_rusage`'s `ri_diskio_byteswritten` on macOS, `/proc/self/io`'s `write_bytes` on Linux,
none elsewhere) and the database data file's length after it. The report's "database writes per
output" section gives each counter that moved as its mean per output (`_ns` counters in ms, the
`tndb_max_gate_lag` maximum as its largest), the disk MiB and the data file's growth per output and
over the run, then deciles (ten consecutive groups of outputs) of the commit's ms, the disk MiB and
the file's length, which show whether the commit grows over a long run. The JSON report (version
4) carries them per row (`db_counters`, `disk_bytes_written`, `db_file_len_bytes`), the source and
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
and still cached, 4 KiB in one read cold at random): in a Linux run on a cached copy, tndb-b's busy
commit grew from 82 to 583 ms over 300 outputs, all in its one `fdatasync` (199 to 532 MiB written
per commit), and MDBX wrote about 970 MiB per commit. Linux commit numbers from runs on a cached copy
are not comparable with later ones. On macOS nothing is evicted: it has no per-file eviction, and no
folio writeback of this kind. On Linux tndb-b also advises its mappings `MADV_RANDOM`
(`EnvOptions::readahead`, off by default there), as reth's MDBX does, so its own faults do not build
large folios.

#### Mac baseline, 2026-10-28

The setup:
- **Machine:** an Apple M1 Max (10 cores, 64 GiB) on its internal SSD (APFS), release builds,
  durable sync on both backends.
- **Run:** `etc/reth-exec-bench.sh` in backend mode, 2 ABBA rounds per profile, the bench's
  defaults:
  - state: 1M prefilled accounts, 10k senders, 64 pools, 200 aging and 20 warm-up outputs;
  - load: 300 timed outputs, back to back, each 8 batches of 15M gas (about 2,400 transactions and
    120M gas per output).
- **Figures:** medians over the rounds, in ms per output. A ratio is tndb-b over MDBX, the median of
  the two pairs' ratios.

| profile | MDBX mean | tndb-b mean | ratio | MDBX p99 | tndb-b p99 | ratio [pairs] | Mgas/s, MDBX / tndb-b |
|---|---|---|---|---|---|---|---|
| `busy` | 801.5 | 852.5 | 1.06 | 1154 | 1358 | 1.18 [0.96..1.40] | 149.2 / 140.2 |
| `busy-readers` (4 readers, the poller, the submitter) | 852.6 | 923.5 | 1.08 | 1213 | 1317 | 1.09 [1.05..1.12] | 140.2 / 129.5 |
| `busy-metrics` (Prometheus, with readers) | 858.2 | 908.6 | 1.06 | 1185 | 1196 | 1.01 [0.96..1.06] | 139.3 / 131.6 |
| `busy-epoch` (a close every 50 outputs) | 803.0 | 849.9 | 1.06 | 1118 | 1216 | 1.09 [0.96..1.22] | 148.9 / 140.7 |
| `busy-export` (a state-pack export beside the writer) | 847.6 | 849.7 | 1.00 | 1299 | 1152 | 0.89 [0.86..0.91] | 141.0 / 140.7 |
| `peak` (20 batches of 30M gas) | 2624 | 3878 | 1.48 | 3642 | 7599 | 2.09 [1.69..2.49] | 228.2 / 154.5 |

Where an output's time goes, `busy`, mean ms:

| stage | MDBX | share | tndb-b | share | ratio |
|---|---|---|---|---|---|
| EVM execution (with signer recovery: 10.9 / 10.8) | 37.0 | 4.6% | 40.6 | 4.8% | 1.10 |
| state root (`finish`: hashed state 1.4, state root 156.2 / 175.6) | 163.8 | 20.4% | 183.1 | 21.5% | 1.12 |
| overlay extend | 5.6 | 0.7% | 5.5 | 0.6% | 1.00 |
| writes (`save_blocks`: the database's writes beside the static files') | 139.0 | 17.3% | 114.6 | 13.4% | 0.82 |
| static-file commit | 34.4 | 4.3% | 33.6 | 3.9% | 0.98 |
| database commit | 419.3 | 52.3% | 472.6 | 55.4% | 1.13 |
| other | 2.5 | 0.3% | 2.5 | 0.3% | 0.99 |

The database's writes split into these stages:

| write stage | MDBX | tndb-b | ratio |
|---|---|---|---|
| `insert_block` | 0.3 | 0.2 | 0.77 |
| `write_state` | 38.5 | 23.2 | 0.60 |
| `write_hashed_state` | 25.6 | 19.6 | 0.77 |
| `write_trie_updates` | 24.1 | 35.9 | 1.49 |
| `update_history_indices` | 38.7 | 29.2 | 0.75 |

What the runs and two `sample` profiles show (one busy run per backend, 120 s inside the timed
loop):
- **The database commit is I/O.** It is half of each output on both backends.
  - On MDBX it is 99.9% `msync`: reth opens MDBX with a write map, so the commit writes back the
    dirty mapped pages. On tndb-b it is 91% `fsync`, which writes back tndb-b's dirty mapped pages.
  - The cost follows the pages a commit dirties, not the logical bytes. MDBX's file grew 616 MiB
    while the export held its read for about 2.6 s (3–4 outputs): about 150–200 MiB of pages per
    output, against 1.1 MiB of logical growth. Random keys over 1M accounts dirty a leaf, and its
    path, per key touched.
  - On macOS the write-back is at the VM page, 16 KiB on Apple Silicon. MDBX uses that as its page
    size (reth's default, the OS page). tndb-b-reth uses 4 KiB pages, so each scattered page it
    dirties writes back 16 KiB.
  - libmdbx's durable sync is `F_FULLFSYNC` and tndb-b's a plain fsync. The Linux run (4 KiB pages,
    `fdatasync`) is needed before ranking the backends on this stage.
- **`peak` is tndb-b's weak spot.** Its commit grows over the run: per-decile means rise from
  1.9 s to 3.25 s, with outputs up to 13 s. MDBX's stays at 1.0–1.3 s. In `busy`, tndb-b's commit
  is about 620 ms for the first ~90 outputs, then about 400 ms (MDBX about 415).
- **The rest of the run:**
  - **History:** apart from tndb-b's early commits, each stage is flat from the first third of the
    timed outputs to the last in `busy`.
  - **Side load:** the readers cost the writer at most 5% (writer on/off 0.97–1.05 per run). Read
    latencies match across the backends: p50 within 5%; `eth_getBlockByNumber` is 0.81x on tndb-b.
  - **Prometheus:** no measurable cost.
  - **Epoch close:** a closing output costs what a normal one does.
- **The export:** it wrote 1.08M accounts, 2.07M storage slots and 25.8k bytecodes (a 152 MiB
  pack) in 2.5–2.7 s (MDBX) and 2.2 s (tndb-b). The writer ran at 1.03x beside it on MDBX and 1.41x
  on tndb-b, over only 2–4 outputs. tndb-b's growth while pinned reads 0: its file grows in
  preallocated steps.

#### Improvement candidates, by measured share (Mac)

1. **The database commit, 52–55%:** the page write-back volume.
   - **Persist every K outputs, not every output.** This amortizes the shared branch pages and the
     hot leaves over K outputs. After a crash, the node would re-execute the outputs since the last
     persist; it already replays the consensus output it missed on restart (`run_epoch`). It is a
     TN design change (the overlay would span K outputs); no fork. The bench can measure it with a
     persist-interval knob.
   - **tndb-b:** an A/B of the page size (16 KiB, the OS page, on macOS) and of allocation locality.
     A delta or WAL commit, writing the changed records and checkpointing pages later, is the
     structural fix: an engine change.
   - **Before tuning,** find what grows tndb-b's commit over a `peak` run.
2. **The state root, 20–22%:** TN calls the serial `StateRoot::root_with_updates`.
   reth-trie-parallel's `ParallelStateRoot` computes the storage roots on rayon. That is a tn-reth
   change; no fork. tndb-b is 12% slower here, in cursor descents (`TreeCursor::descend`, 9% of an
   output).
3. **The database's writes, 13–17%:**
   - **MDBX:** libmdbx's `mincore` residency check on page touch is 6.4% of an output.
   - **tndb-b:** zero-filling each new page (`memset` in `Writer::alloc`) is 4.7% of an output, a
     third of the write stage.
   - **tndb-b's dup-sort emulation:** reth's writers do `seek_by_key_subkey`, `delete_current`,
     then `upsert` per changed slot and trie node, where one put would do. The seek and the delete
     are about 4% of an output (`delete_current` 3.1%, the seek 1.1%), and `write_trie_updates`
     runs at 1.49x MDBX.
   - **The fork question:** this stage is the one a reth fork could change. A one-call dup put
     would remove the seek and the delete. The adapter can first try a cached seek position,
     without a fork.
4. **The static-file commit, 4%:** NippyJar's `File::sync_all` on every persist, which is
   `F_FULLFSYNC` on macOS. It is reth code (a fork or an upstream change). Linux's `fsync` should
   cost much less (not measured yet).
5. **EVM execution, 5%,** with signer recovery at 1.4%. `CachedReads` is dropped every block
   (`env/execution.rs`), so the hot token and router accounts are read again by each of an
   output's blocks.

The two largest stages (the commit's I/O, the state root) are reachable without forking reth. The
parts a fork would change (the write pattern, about 4% on tndb-b, and the static-file sync, 4%)
are smaller. The Linux run (the user's, with the same script) decides the commit ranking.

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
    database open (and, on tndb-b, locked) until it drops too.
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
  `mdbx_newly`, `mdbx_cow`, `mdbx_msync`, ...) or tndb-b's `IoStats` (as `tndb_commits`,
  `tndb_pages_synced`, ..., times in `_ns`); `since` subtracts an earlier read, so a read before
  and after an output gives what it wrote. A read is one `mdbx_env_info_ex` or one short lock of
  tndb-b's published copy. `db_page_size(&db)` gives the page size the file was created with.
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
    derived datadir, such as an aged one, appends its own `extra` component. `LAYOUT_VERSION` is 2
    since the state tables became the engine's dup tables on tndb-b, which renamed every base:
    each backend's bases, and their aged children, rebuild once (a tndb-b base of layout 1 holds
    those tables composite, which the open refuses).
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
    accounts and 10k senders):

    | backend | build | genesis and chain spec | `init_genesis` | data file allocated | peak RSS |
    |---|---|---|---|---|---|
    | MDBX | 12.9 s | 4.2 s | 8.6 s | `mdbx.dat` 712 MiB | 4.4 GB |
    | tndb-b | 13.2 s | 4.2 s | 9.0 s | `tndb.dat` 1040 MiB | 4.7 GB |
- **`workload`:** `Workload::new(env, spec, WorkloadConfig)` draws a seeded mix from the senders'
  on-chain nonces: by default 40% native transfers, 40% ERC20 transfers, 15% swaps and 5% 1 KiB
  creations, with 20% fresh recipients and the rest Zipf-hot `account(i)`. `next_batch(target_gas)`
  signs a `LoadBatch` filled by exact expected gas, each gas limit 10% over it and within
  `block_gas_limit` (30M). Gas on the test chain:

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
| `src/env/database.rs` | The database backend (`RethDbEnv`/`RethDb`, MDBX or tndb-b): `open_rw`, the backend guard, the `--db.*` mapping, the table stats of `db stats` (`db_table_stats`; see "Database backend"). `src/env/database/migrate.rs`: the MDBX to tndb-b migration (tndb build). |
| `src/env/epoch.rs` | Startup healing and epoch-boundary reads: `EpochState`, committees, BLS pubkeys, worker fee configs, classified registry reads (node-local vs chain-global failures). |
| `src/env/execution.rs` | `build_block_from_batch_payload` (recover → execute → assemble), `finish_executing_output` (atomic blocks + finality markers), `finalize_block` (in-memory watches). |
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
| `src/metrics.rs` | Block-building drop counters (`unrecoverable` alertable, `invalid` expected), the per-block stage timings, and the epoch-close system-call gas gauges. |
| `src/payload.rs` | `TNPayload` and `BuildArguments`: consensus data shaped for execution. |
| `src/rpc_server_args.rs` | RPC CLI argument subset and transport-limit defaults. |
| `src/snapshot.rs` | State-pack export (`PinnedStateView`) and verified restore (`SnapshotRestorer`). |
| `src/system_calls.rs` | `sol!` bindings for `ConsensusRegistry`/`WorkerConfigs`, `SYSTEM_ADDRESS`, registry address, `EpochState`. |
| `src/traits.rs` | `TelcoinNode` node-type wiring and the fail-loud `TNExecution` shim. |
| `src/txn_pool.rs` | `WorkerTxPool` wrapper: canonical-state maintenance, blob-tx removal, raw-tx recovery helpers. |
| `src/types.rs` | Type aliases (`RpcServer`, `RethDb`, `PoolTxn`, `TNPrimitives`); `RethDb` is re-exported from `src/env/database.rs`. |
| `src/worker.rs` | `WorkerComponents` and the `WorkerNetwork` RPC shim (libp2p peer count for `net_*`). |
| `src/test_utils.rs` | `TransactionFactory` and payload-execution helpers (`test-utils` feature / tests). |
| `src/test_utils/load/` | Shared code for the busy-node execution bench (`test-utils`): the capture recorder, the production env and datadir clones (`env`), the database's counters and page size (`db_counters`), the prefilled state (`state`) and its cached base (`base`), the transaction mix (`workload`), the consensus outputs (`output`) and the RPC side load (`readers`). See "Execution benchmark". |

## Feature flags

From `crates/tn-reth/Cargo.toml`:

| Feature | Effect |
|---|---|
| `faucet` | Instant role-gated TEL mint replacing the timelocked mint. **Never on mainnet.** |
| `adiri` | Adiri-testnet build: forwards `tn-types/adiri`, which compiles the adiri fork gates in `tn-types` `forks.rs`, and **implies `faucet`**. Each gate names the first adiri epoch on the new behaviour: 383 seed signatures (epoch seed chain replaces the leader-aggregate shuffle seed), 407 `ConsensusRegistry` and `WorkerConfigs` swap, 554 governance Safe onto the canonical v1.4.1 suite, 567 leader-seeded intra-round certificate order, 570 multi-worker `Committee` encoding, 574 seed-chain `PREVRANDAO`. This crate applies the two state-swap forks (407 and 554) itself, in the closing block of the epoch before (steps 1 and 2 of "Epoch close"). Non-adiri builds compile none of these constants and run the post-fork behaviour from genesis. |
| `test-utils` | Test factories and payload helpers. |
| `rocksdb` | Opt back in to reth's RocksDB backend (storage is MDBX-only today). Turns on the `rocksdb` features of `reth-node-core`, `reth-provider`, `reth-db-common`, `reth-stages` and `reth-prune`. |
| `tndb-b` | Run the reth execution database on the tndb-b engine (`tndb_b_reth::TndbEnv`) instead of MDBX. Off by default; see "Database backend". |

The crate takes reth's individual crates, never the `reth` umbrella crate (which only re-exports
them and pulls reth's CLI, `reth-ethereum-cli` and `reth-cli-commands`, into the graph). The
umbrella's default features are always on, enabled on the crates it forwarded them to: `jemalloc`
(`reth-cli-util`, `reth-node-core`; not `reth-node-metrics`, whose jemalloc stats feed only
reth's own metrics server, which TN never starts), `otlp` and `otlp-logs` (`reth-node-core`), `js-tracer`
(`reth-node-builder`, `reth-node-ethereum`, `reth-rpc`, `reth-rpc-eth-types`),
`keccak-cache-global` (`reth-node-core`, `reth-node-ethereum`), `asm-keccak` (`reth-node-core`,
`reth-node-ethereum`, `reth-primitives`) and `min-debug-logs` (`reth-node-core`, which caps
`tracing` at debug in release builds).
