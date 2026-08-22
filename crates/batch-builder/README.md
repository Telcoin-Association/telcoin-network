# Batch Builder

## Purpose & Scope

The batch builder selects transactions from the node's transaction pool and assembles them into batches that extend the current canonical tip.
It operates as a Future-based task that coordinates with the consensus layer to ensure batches are only mined after successfully reaching quorum of support from other workers.

## Key Components

### BatchBuilder

- **Core Structure**: Implements the `Future` trait for asynchronous batch assembly
- **Mining Logic**: Selects transactions from the local transaction pool based on current basefee and epoch parameters
- **Lifecycle Management**: Automatically shuts down at epoch boundaries to maintain protocol synchronization

### Batch Assembly Process

Validator nodes maintain unique transaction pools.
Transactions are not gossiped, but are distributed as sealed batches of transactions for other validators to validate.

#### Transaction Selection

Each worker selects the best transactions from their respective transaction pools to include in the next proposed batch.
Workers only propose one batch at a time, and transactions are only removed from the pool once the batch that contains them reaches quorum (2f+1).

Transactions are sorted by default based on the highest fees, although this is not a strict requirement of the protocol.

The only requirement is that transactions must extend the current canonical tip.
The canonical tip is extended once a batch is settled in a DAG commit.
The entire collection of batches is then sent to the `tn-engine` for final execution.

#### Validation

The logic for validating batches is in the `batch-validator` library.
If a node includes a batch that was not validated by the worker's peers, the Primary's `Header` will fail validation.

## Pool State Updates and Nonce Tracking

### Problem

The batch builder operates without executing transactions.
After a batch reaches quorum and transactions are removed from the pending pool, the pool's internal sender nonce tracking must be updated.
Without this, the pool perceives a nonce gap for remaining transactions from the same sender and demotes them from `pending` to `queued`.
Since the batch builder only pulls from the `pending` sub-pool, demoted transactions stall until the engine's canonical update corrects the pool state, which can take minutes depending on consensus round timing.

This is particularly impactful for sequential deployment scripts (e.g. forge/cast) that submit hundreds of transactions from a single sender.

### Solution: Early Nonce Updates via `changed_accounts`

After building a batch, `build_batch()` tracks the highest nonce included per sender and returns a `changed_accounts` vector alongside the mined transaction hashes.
When the batch reaches quorum, the batch builder calls `update_canonical_state()` with both:

1. **`mined_transactions`**: hashes of transactions to remove from the pool
2. **`changed_accounts`**: per-sender nonce updates (highest mined nonce + 1) so remaining transactions from the same sender stay in `pending`

This allows the batch builder to immediately loop and build the next batch from the same sender's remaining transactions without waiting for the engine.

### Two-Layer Pool Update Architecture

The pool receives updates from **two independent sources**:

1. **Batch builder** (`lib.rs` → `update_canonical_state()`): Called immediately after quorum. Provides an early, optimistic update with mined transaction hashes and nonce advances. Carries each sender's real canonical balance (see below).

2. **Canonical pool task** (`txn_pool.rs` → `process_canon_state_update()`): A separate background task subscribed to the engine's canonical state stream. Called after the engine executes the committed batches and produces a new canonical block. Provides authoritative nonce, balance, and mined transaction data derived from actual EVM execution.

Both call `on_canonical_state_change()` on the underlying Reth pool. The engine's update overwrites the batch builder's optimistic state with the real post-execution values.

### Which Balance the Optimistic Update Carries

The batch builder does not execute transactions and cannot know the post-execution balance.
The balance field in `ChangedAccount` is set to the sender's **real canonical balance minus the cost of the transactions just mined for that sender** (balance read via `TxPool::get_account_balance`), not `U256::MAX`.

- **`U256::MAX` is an amplification vector.** The optimistic balance decides whether the pool *promotes existing* parked transactions after mining. With `U256::MAX`, a sender's queued insufficient-funds transactions all look affordable, so the pool promotes them and the batch builder packs them into the next batch built inside the optimistic window — before the engine's canonical update corrects the balance. Peer batch validation is purely structural, so that follow-up batch reaches quorum, is gossiped and stored, and is then skipped for free at execution. A funded attacker pays for roughly one transaction per sender but forces the network to certify and store up to `TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER - 1` extra invalid transactions per sender.
- **Debiting the just-mined cost stops the burst.** Because the mined transactions' cost is subtracted from the reported balance, a sender that cannot actually fund its remaining transactions keeps them parked. The immediate amplification the issue's PoC relies on — queue many transactions, mine one, watch the rest promote into the very next batch — no longer occurs.
- **Residual (known limitation).** The balance is re-derived from the last committed canonical balance on every in-window rebuild and does not accumulate debits across successive rebuilds before the engine's canonical update lands. A drip-fed sender can therefore still have on the order of one transaction promoted per rebuild until the canonical update corrects the balance. Fully closing this requires tracking cumulative optimistic spend across rebuilds, or a state-aware check on the follow-up batch path (directions 3 and 4 in issue #1158); both are larger design choices deferred to the maintainers.
- **Entry validation is unaffected.** New transactions entering the pool are still validated against the real on-chain balance by Reth's `TransactionValidationTaskExecutor`. The `changed_accounts` balance only affects promotion/demotion of transactions already in the pool.
- **Short persistence window.** The optimistic balance only persists until the engine's canonical update arrives (same consensus round), which overwrites it with the real post-execution balance.
- **No unnecessary demotion for legitimate senders.** A sender whose remaining transactions are affordable at their real balance keeps them in `pending`; only genuinely underfunded transactions stay parked, which is correct.
- **Conservative on read failure.** A missing account or a state-read error yields `U256::ZERO`, which can only keep transactions parked (never promote an unfunded one); the engine's authoritative update corrects it the same round.

## Security Considerations

### Threat Models

#### MEV

Transaction pools are isolated to individual nodes, which prevents anonymous MEV attacks.
Validator nodes must obtain an NFT through decentralized governance and are "well-known".

#### Invalid Transactions

Transactions are not gossiped until they are sealed in batches.
It's possible for different nodes to include duplicate transactions in their batches or transactions that attempt to double-spend.
Once the batches reach consensus, they are ordered deterministically by the Primary using `Bullshark` and executed by `tn-engine`.
Invalid transactions fail at execution.
This is a non-fatal error.
Although this is inefficient, it is considered an acceptable limitation of the protocol at this time.
Future iterations are planned to address this inefficiency.

### Safety of Early Pool Updates

#### Quorum failure does not corrupt pool state

On quorum failure (any of `QuorumRejected`, `AntiQuorum`, `Timeout`, `NotValidator`, `FailedQuorum`), the spawned task returns `MinedBatchResult` with empty `mined_transactions` and empty `changed_accounts`.
The batch builder checks `mined_transactions.is_empty()` and skips the `update_canonical_state()` call entirely.
Pool state is unchanged and the original transactions remain in `pending` for the next attempt.

#### Cross-header ordering is guaranteed by Bullshark

Bullshark consensus commits are causally ordered and irreversible.
Batches must reach quorum *before* header inclusion.
Headers only include the local worker's batches.
If Header N+1 commits, all ancestor headers (including Header N) are already committed.
The scenario where "batch with nonces 0-7 fails but batch with nonces 8-15 succeeds" is impossible:

- If quorum fails: pool is not updated, same transactions are re-included in the next batch attempt
- If quorum succeeds: the batch is in a header that will be committed before any dependent header

#### No race between batch builder and canonical update

The batch builder's early update and the engine's canonical update both call `on_canonical_state_change()` on the Reth pool, which acquires an internal write lock.
These updates are serialized.
The engine's update is authoritative and overwrites the batch builder's optimistic nonce/balance values.
The worst case is a brief window where the pool has the optimistic values, which is the intended design.

#### Fatal error causes shutdown

`FatalDBFailure` from the worker propagates as `Err` through the `BatchBuilder` future, shutting down the batch builder.
No pool update occurs.

### Trust Assumptions

- Assumes the Worker will continue processing a single batch until it reaches quorum
  - This is verified when validating Primary Headers
- Relies on the execution engine to handle invalid transactions post-consensus
- Trusts basefee calculations are correctly applied at epoch boundaries
- The engine's canonical update is the authoritative source of truth for pool state; the batch builder's update is an optimization for throughput

### Critical Invariants

- Transactions are only removed from the transaction pool if the batch reaches quorum
- `changed_accounts` is only applied when `mined_transactions` is non-empty (same guard)
- The engine's `process_canon_state_update()` always runs independently and overwrites the batch builder's optimistic state
- Transaction pool state remains consistent with canonical chain execution AND batch execution
  - Transactions in a batch must always extend the canonical tip, not the preceding batch
- Basefees only adjust at the start of a new epoch
- Epoch boundary synchronization is maintained across all batch builders (currently TN only supports 1 per node)

## Dependencies & Interfaces

### Dependencies

- **Transaction Pool**: Source of candidate transactions for batch assembly (see tn-reth library)
- **Canonical Chain State**: Current canonical tip updates from `tn-engine`
- **Epoch Management**: Basefee and epoch boundary information

### Interfaces

- **Outbound to Worker**: Sealed batches (batch with hashed digest) are sent to the worker for consensus processing
- **Inbound from Consensus**: Quorum achievement signals that trigger mining operations
- **Validation**: Peers validate batches from other peers (with matching `WorkerId`s) using logic in the `batch-validator` lib
