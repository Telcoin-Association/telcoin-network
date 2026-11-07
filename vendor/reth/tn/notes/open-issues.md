# Open issues: known, not fixed in the vendoring PR

The PR that brought `vendor/reth` onto `main` fixed what its reviews found that was small and
safe to change before the testnet upgrade. The items below are known and deliberately left for
their own changes, because each is too large for that PR or needs other owners. None of them lets
two validators execute a block differently.

Upstream fixes that are simply not applied yet are tracked in `upstream-backports.md`, not here.

## The pool re-admits a nonce from a batch that reached quorum (review finding T2-1)

After a batch reaches quorum, the batch builder advances each sender's nonce optimistically and
removes the batched transactions from the pool (`crates/batch-builder/src/batch.rs`,
`crates/batch-builder/src/lib.rs`). Until the batch executes, canonical state still holds the old
nonce. The pool validates new transactions against canonical state
(`vendor/reth/crates/transaction-pool/src/validate/eth.rs`), and `TxPool::add_transaction`
overwrites the sender's tracked nonce with it (`pool/txpool.rs`). So a second transaction with the
nonce that was just batched enters the pool as pending, and the next batch carries it.

Peers validate batches structurally only (`crates/batch-validator/src/validator.rs`), and
execution skips the copy as invalid without charging the sender
(`crates/tn-reth/src/env/execution.rs`). A sender pays only for the first transaction with each
nonce; each free copy can carry up to 128 KiB of calldata and outranks honest transactions by its
unpaid tip. Skipped copies add nothing to `gas_used`, so the base fee does not react.

- Fork risk: none. Every validator skips the same transactions.
- Not new: TN had the same behaviour on the reth git dependency.
- Fix: backport upstream 75d587208 (`enforce_tracked_nonce`) and enable it in
  `WorkerTxPool::build` (`crates/tn-reth/src/txn_pool.rs`). Before that, the paths that re-add
  transactions below the optimistic nonce must first reset the sender to canonical state, as
  `reload_dirty_accounts` does: the orphan-batch repool at epoch close
  (`crates/node/src/manager/node/close_epoch.rs`), the observer forward requeue and
  `retry_forwarded` (`txn_pool.rs`), and a quorum batch whose header never commits. A canonical
  update for an earlier block that touches the sender also lowers the tracked nonce again
  (`txpool.rs`), so a complete fix makes that update monotonic. Needs the batch-builder owners.

## RPC work shares the blocking pool with execution and consensus

reth's RPC and TN's execution engine and certificate validation all run on tokio's one blocking
pool (512 threads by default). Semaphores bound most RPC work: upstream's for `eth_call`,
`eth_callMany`, `eth_simulateV1`, `eth_getLogs` and `eth_getProof` (TN-0019, TN-0035), and TN's for
`eth_estimateGas`, `eth_fillTransaction`, `eth_createAccessList`, `eth_callBundle` (TN-0037,
`crates/tn-reth/src/rpc_call.rs`), `debug_trace*` (TN-0036), and `trace_*` and
`debug_executionWitness*` (TN-0038). One gap remains: long synchronous jobs (a large `eth_call`)
run to completion after the client leaves. Upstream 13a59de4b and 621f663ed stop them; they need
`CancelOnDrop` moved into reth-tasks first (`upstream-backports.md`, Medium).

The structural fix is to give the RPC `TaskSpawner` (`crates/types/src/task_manager.rs`) its own
runtime or bounded blocking pool, so RPC load cannot delay block execution whatever each method
does.

## RocksDB stubs and storage-v2 plumbing

RocksDB itself is gone (TN-0014), but the zero-size stub types, the provider-factory field and
the `EitherWriter`/`EitherReader` parameters remain. Removing them changes compiled storage code,
so it waits for the storage work (DECISIONS.md D15; ROADMAP.md, "RocksDB step B").

## MDBX fixes belong to the storage work

Upstream MDBX fixes (7bf9241fe, 294e21507, 5fc3a81d3 in `upstream-backports.md`) are tracked for
the tndb-b owners, who replace MDBX as the reth database (ROADMAP.md, Phase 3).

## The revm-inspectors patch

`patches/revm-inspectors` lowers Boa's recursion limit in the revm-inspectors version TN's revm
needs (0.34.3). It retires when TN's reth moves to a revm-inspectors release that has upstream's
fix (0.44.2 or later). `patches/revm-inspectors/PATCH.md` has the
details.

## reth's test-only dependencies are not pinned

`tn/cargo.sh` seeds the nested lockfile from TN's on each run, and cargo resolves the packages only
reth's tests need (about 75) against crates.io at that moment. TN's shipped code is unaffected,
but the `test-vendor-reth` lane is not reproducible from a commit, and a bad release of one of
those packages can fail it for every queued PR. `cargo.sh` limits them to versions that support
the workspace's `rust-version`, which removes the commonest break (a release that needs a newer
rustc). The complete fix is a committed overlay lockfile for the nested workspace, run with
`--locked`, plus a check that its shared packages match TN's lockfile. That reverses DECISIONS.md
D7, so it is its own change.

## CI build time with the vendored crates

rust-cache does not save path packages inside the repository, so every test lane now rebuilds the
57 vendored crates, which the git dependency had cached, and `vendor-reth-test` compiles reth
four ways. None of this has been timed on a GitHub runner. Before queueing the PR, a dispatch of
`main-compiles.yaml` on the branch times the builds; caching the vendored crates would need
sccache or a different cache layout.

## Ledger coverage is checked by hand

`vendor.py status` lists the files that differ from upstream, and CHANGES.md must account for
each one, but no tool or CI job compares the two. A `vendor.py ledger` check (every changed file
named by an entry, every `TN(TN-NNNN)` id defined) would make that mechanical.

## The upstream survey is from 2026-10-30

`upstream-backports.md` surveyed upstream `main` up to `84caec584`. Upstream has moved since; the
next survey starts there (CLAUDE.md, "Tracking upstream reth").

## Branches that carry tndb-b

The tndb-b work was written against an earlier copy of this vendoring on another branch. Rebasing
it onto `main` meets this copy's changes: it must re-add its own workspace dependencies to the
root `Cargo.toml`, and it meets `patches/revm-inspectors` and the `--config` patch line in
`tn/cargo.sh`.
