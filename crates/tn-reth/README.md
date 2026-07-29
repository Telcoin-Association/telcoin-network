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
  block (its `ommers_hash` is zero — see `is_worker_batch_block` in `src/snapshot.rs`).
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

## Header field mapping

TN repurposes several Ethereum header fields for protocol data. Assembly happens in
`TNBlockAssembler::assemble_block` (`src/evm/block.rs`) from the `TNBlockExecutionCtx` built in
`src/evm/config.rs`; values originate in `TNPayload` (`src/payload.rs`).

| Field | TN meaning |
|---|---|
| `nonce` | `((epoch as u64) << 32) \| round` of the leader certificate (`tn-types` `primary/header.rs`; decoded by `deconstruct_nonce`). Epoch = high 32 bits, round = low 32 bits. |
| `difficulty` | `batch_index << 16 \| worker_id`. **`worker_id` is the LOW 16 bits; `batch_index` occupies the upper bits** (`TNBlockExecutionCtx` docs and `context_for_next_block` in `src/evm/config.rs`). `first_batch()` exploits this: `difficulty < 65536` ⇔ `batch_index == 0`. `worker_id_from_header` in `src/snapshot.rs` reads the low 16 bits. |
| `mix_hash` | Computed in `crates/engine/src/payload_builder.rs`: `output_digest ^ batch_digest` when the output has batches, plain `output_digest` otherwise. Exposed as `prevrandao` (EIP-4399). |
| `extra_data` | Empty for a normal block. For an epoch-closing block: the 32-byte keccak256 of the leader certificate's aggregate BLS signature. The replay path (`context_for_block` in `src/evm/config.rs`) accepts only length 0 or 32 and errors on anything else. |
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
  environment prices blob gas at `u128::MAX` (`next_evm_env`), the pool's canonical-state update
  passes `pending_block_blob_fee = Some(u128::MAX)` (`src/txn_pool.rs`), and the batch builder
  (`crates/batch-builder/src/batch.rs`) marks blob transactions invalid via
  `BestTxns::ignore_eip4844` and purges them and their descendants with
  `WorkerTxPool::remove_eip4844_txs` (which also deletes sidecars from the blob store).
- The in-protocol `ConsensusRegistry` upgrade is gated by `CONSENSUS_REGISTRY_FORK_EPOCH`
  (`tn-types` `forks.rs`, currently the `u32::MAX` placeholder) and compiled only under the
  `adiri` feature. See "ConsensusRegistry fork gate" below.

## Epoch close

When `ctx.close_epoch` is set (last batch of the last output in an epoch), the block executor's
`finish()` (`src/evm/block.rs`) runs this ordered sequence, **every step fatal to the block on
failure**:

1. *(adiri builds only)* If the epoch being concluded satisfies
   `concluding_epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH`, apply the registry fork
   (`apply_consensus_registry_fork`) — code swap plus one-time `migrateValidatorSets()` — so the
   remaining steps in this same block already run on the upgraded code.
2. `applyIncentives` on the `ConsensusRegistry` with each validator's consensus-leader count
   (`apply_consensus_block_rewards`).
3. `concludeEpoch` with the new committee (`apply_closing_epoch_contract_call`): the eligible pool
   (union of `Active`, `PendingActivation`, `PendingExit`) is read from the registry, shuffled by
   a Fisher-Yates over an `StdRng` seeded with the epoch-close randomness, backfilled from
   pending-exit validators if the active set is short, and truncated to
   `getNextCommitteeSize()`. An undersized pool fails client-side with
   `TnRethError::UndersizedCommittee` instead of submitting calldata that reverts on-chain.
4. `merge_transitions(BundleRetention::Reverts)` folds the system-call state into the bundle.

System calls execute as `SYSTEM_ADDRESS` → contract with a fixed 30M gas limit, zero gas price,
base-fee and nonce checks disabled (`transact_system_call` in `src/evm/mod.rs`);
`SYSTEM_ADDRESS` is stripped from the changeset before commit so it never enters the state root.

**Slashing is not live.** `applySlashes` is declared in the `sol!` interface
(`src/system_calls.rs`, documented "Currently disabled during MNO pilot") but no Rust code
anywhere in this repository calls it.

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
- **`BASEFEE_ADDRESS` is a process-global `OnceLock`** (`src/lib.rs`), written once by
  `set_basefee_address` during `RethEnv::new` (`src/env/mod.rs`). The first write wins; later
  writes are **silently discarded** (the `set` error is intentionally ignored). If it is never
  set, `basefee_address()` falls back to `GOVERNANCE_SAFE_ADDRESS`. Every node in the fleet must
  agree on this value — a node with a different base-fee address computes different state roots
  and forks off (the doc comment on `set_basefee_address` says exactly this).

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
  consensus layer signs with, for a flat 150,000 gas. `ConsensusRegistry` staticcalls it for
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

- **Namespace allowlist** (`src/cli.rs`): the constant `ALL_MODULES` permits exactly
  `eth, net, web3, debug, trace, rpc`. `RpcModuleSelection::All` is rewritten to that set;
  explicit selections are intersected with it; requests for `admin` or `txpool` are dropped with
  a warning. Applied to both HTTP and WS.
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

### `SYSTEM_ADDRESS` is unreachable by users

`SYSTEM_ADDRESS` (`0xff…fe`, `src/system_calls.rs`) is crate-private and has no known private
key, so no signed transaction can recover to it. It appears as `caller` only in internally
constructed system-call transactions: the block executor's state-changing epoch calls and the
read-only registry/config queries (`src/evm/block.rs`, `src/env/epoch.rs`). State-changing
callers strip it from the changeset before commit, and `TNEvmHandler` skips fee accounting for
it so the beneficiary and base-fee accounts are never spuriously touched.

### Snapshot export / restore

`src/snapshot.rs`. Export (`PinnedStateView`) streams plain state from a pinned read-only MDBX
transaction into a CRC32+zstd state pack; it records the caller-supplied state root **without
recomputing it**. The restore side is the real integrity check (`SnapshotRestorer`):

- refuses any datadir that already holds chain data; genesis (the trust root) always comes from
  the local chain spec, never the snapshot;
- scaffolds a header-only chain (real hashes inside the shipped window, zero-hash dummies below);
- **recomputes the state root from scratch** out of the imported accounts (`StateRoot::from_tx`)
  and hard-fails unless it equals both the pack's declared root and `header(B).state_root` from
  the caller-supplied header window;
- verifies the shipped window can seed the first epoch-entry base-fee derivation for every
  EIP-1559-configured worker (otherwise the restored node would walk into omitted history and
  halt);
- persists finalized/safe markers at `B` and re-checks the reconstructed tip number and hash.

What restore does *not* verify: the authenticity of the header window itself — that is the
caller's trust decision (the window is validated upstream against consensus data).

## Determinism rules

Block production must be a pure function of certified consensus output. Concretely:

- No wall-clock reads in state-affecting paths; block timestamps come from the consensus commit
  timestamp carried in the payload.
- No hash-map iteration in state-affecting paths — rewards use `BTreeMap`, committees are sorted
  by address before encoding (`generate_conclude_epoch_calldata`).
- Epoch-close randomness is `keccak256` of the leader certificate's aggregate BLS signature,
  computed once in consensus (`tn-types` `primary/output.rs`) and carried in the payload. It
  seeds the committee-shuffle RNG **and** is stored in `extra_data`, so production
  (`context_for_next_block`) and replay (`context_for_block`) derive identical state.
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
| `src/env/epoch.rs` | Startup healing and epoch-boundary reads: `EpochState`, committees, BLS pubkeys, worker fee configs, classified registry reads (node-local vs chain-global failures). |
| `src/env/execution.rs` | `build_block_from_batch_payload` (recover → execute → assemble), `finish_executing_output` (atomic blocks + finality markers), `finalize_block` (in-memory watches). |
| `src/env/genesis.rs` | Genesis construction, including `ConsensusRegistry` account generation via a pre-genesis EVM and embedded-artifact JSON parsing; temp-chain env for tests. |
| `src/env/helpers.rs` | Read-side accessors: headers, blocks, receipts, accounts, `read_contract*`, canonical streams and tips. |
| `src/env/rpc.rs` | Assembles and starts reth's RPC server over the worker pool + `WorkerNetwork`; no engine namespace. |
| `src/error.rs` | `TnRethError` and the state-read error taxonomy. |
| `src/evm/mod.rs` | `TNEvm` wrapper over revm; `transact_system_call` (30M gas, fee-exempt, nonce-check disabled); pre-genesis create. |
| `src/evm/block.rs` | `TNBlockExecutor` (pre-block system contracts, epoch-close sequence, registry fork) and `TNBlockAssembler` (header assembly); committee assembly. |
| `src/evm/config.rs` | `TnEvmConfig`: EVM env derivation, difficulty packing, `extra_data` decode for replay. |
| `src/evm/context.rs` | revm context type aliases and builder traits. |
| `src/evm/factory.rs` | `TNEvmFactory` / `TNBlockExecutorFactory`; installs TEL + BLS precompiles on every EVM instance. |
| `src/evm/handler.rs` | `TNEvmHandler`: base-fee crediting and the gas-limit penalty. |
| `src/evm/utils.rs` | `calculate_gas_penalty`. |
| `src/evm/tel_precompile/` | Native TEL issuance at `0x…07e1` (see its README). |
| `src/evm/bls_precompile/` | BLS12-381 signature verification at `0x…b151`. |
| `src/forward.rs` | `WorkerRpcForwarder`: observer → committee transaction forwarding. |
| `src/metrics.rs` | Block-building drop counters (`unrecoverable` alertable, `invalid` expected). |
| `src/payload.rs` | `TNPayload` and `BuildArguments`: consensus data shaped for execution. |
| `src/rpc_server_args.rs` | RPC CLI argument subset and transport-limit defaults. |
| `src/snapshot.rs` | State-pack export (`PinnedStateView`) and verified restore (`SnapshotRestorer`). |
| `src/system_calls.rs` | `sol!` bindings for `ConsensusRegistry`/`WorkerConfigs`, `SYSTEM_ADDRESS`, registry address, `EpochState`. |
| `src/traits.rs` | `TelcoinNode` node-type wiring and the fail-loud `TNExecution` shim. |
| `src/txn_pool.rs` | `WorkerTxPool` wrapper: canonical-state maintenance, blob-tx removal, raw-tx recovery helpers. |
| `src/types.rs` | Type aliases (`RpcServer`, `RethDb`, `PoolTxn`, `TNPrimitives`). |
| `src/worker.rs` | `WorkerComponents` and the `WorkerNetwork` RPC shim (libp2p peer count for `net_*`). |
| `src/test_utils.rs` | `TransactionFactory` and payload-execution helpers (`test-utils` feature / tests). |

## Feature flags

From `crates/tn-reth/Cargo.toml`:

| Feature | Effect |
|---|---|
| `faucet` | Instant role-gated TEL mint replacing the timelocked mint. **Never on mainnet.** |
| `adiri` | Adiri-testnet build: enables the registry fork machinery and **implies `faucet`**. |
| `test-utils` | Test factories and payload helpers. |
| `rocksdb` | Opt back in to reth's RocksDB backend (storage is MDBX-only today). |
