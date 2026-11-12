# Open issues: known, not fixed in the vendoring PR

The PR that brought `vendor/reth` onto `main` fixed what its reviews found that was small and
safe to change before the testnet upgrade. The items below are known and deliberately left for
their own changes, because each is too large for that PR or needs other owners. None of them lets
two validators on the same release execute a block differently. The first one is fixed now
(TN-0140); the fix changes the state root of any block where the old code was wrong, so it ships
with a coordinated upgrade.

Upstream fixes that are simply not applied yet are tracked in `upstream-backports.md`, not here.

## The incremental state root can skip a changed subtree (upstream 8572c88fe) [fixed: TN-0140]

Applied as TN-0140 with the storage-v2 work. Mainnet takes it at genesis; a chain an older binary
built (adiri) is replayed with the fix and its block hashes compared before the fix is deployed
there (CHANGES.md TN-0140). The analysis below is from before the fix.

`TrieNodeIter` (`vendor/reth/crates/trie/trie/src/node_iter.rs`, the `can_skip_node` seek check)
skips re-hashing a stored branch when the walker's key merely *starts with* the seek prefix. When
the stored key is deeper than the prefix, hashed entries between the two are never visited: a leaf
inserted there (upstream's test inserts storage slot `0x1000..` beside stored branches under
`0x10a0..`) is left out of the root. Upstream fixed it on 2026-11-03 by skipping only at the exact
prefix. The line is the same in reth v1.11.3, which `main` builds, and in this copy.

TN computes every block's root this way: `OutputTrieOverlay::layered_root_with_updates`
(`crates/tn-reth/src/env/output_overlay.rs`) runs `StateRoot` over stored trie nodes with prefix
sets from the block's own changes. Upstream's test fails on this copy (incremental root `0x52fe…`,
rebuilt root `0x6b94…`) and passes with the one-line fix.

- Fork risk today: none between nodes on the same release. The skip is deterministic, so every
  node that built its trie by executing the same blocks computes the same root, right or wrong. A
  node whose trie was rebuilt from scratch (snapshot import, `crates/tn-reth/src/snapshot.rs`)
  hard-fails at import if the chain's root ever went wrong, instead of forking.
- Fork risk of the fix: any block where the case occurs gets a different root on a fixed binary.
  So the fix is a coordinated upgrade, never a rolling one, and it is not applied here.
- Has the testnet hit it? Unknown. A snapshot export from a testnet node and an import elsewhere
  answers it: the import rebuilds the trie from scratch and compares with the header's root. A
  replay of the testnet from genesis on a fixed binary answers it too, and names the first block.
- Fix: apply 8572c88fe at a coordinated upgrade height (or with a height gate if the testnet's
  history already contains such a block), after `graph.py` and `make test-upgrade`.

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
- Design notes from the 2026-11-08 review, for the TN half (`tasks/t2-1-plan.md`):
  - R11, the RPC-visible symptom: with two of a sender's batches in flight (nonces k and k+1
    certified, tracked nonce k+2), the canonical update for the first batch overwrites the
    tracked nonce to k+1; `get_highest_consecutive_transaction_by_sender` then stops at the gap
    and `eth_getTransactionCount(pending)` falls back to the on-chain k+1 until the second batch
    executes. A wallet that signs k+1 again in that round produces a duplicate the pool admits
    and execution skips for free, the T2-1 mechanism. The floor/clamp design (tracked nonce
    monotonic while a quorum batch is unexecuted) covers it; a narrower stop-gap in TN's wrapper
    iterates from the pool's lowest nonce for the sender when it is above the tracked one.
  - With `enforce_tracked_nonce` on, a sender whose only transactions were orphaned keeps an
    advanced tracked nonce that `unique_senders()` cannot surface, so the epoch-start reload
    misses it and the orphan repool is refused with `NonceNotConsistent` and the batch deleted
    from the cache; the pool needs a `sender_info` accessor or reset entry point (the observer
    requeue has the same gap).
  - `keep_sender_info` makes `sender_info` insert-only, one entry per sender ever admitted (the
    growth class of `SenderIdentifiers`); prune on floor release.

## Storage v2 corner cases with no TN caller, or a halt behind them

Known and left alone, because TN never takes these paths or stops the node first. Each needs its
own change if that stops being true.

- **`take_block_and_execution_above` returns the wrong post-state under v2.** It calls
  `unwind_trie_state_from` first, which reverts `HashedAccounts` and `HashedStorages`, and then
  `take_state_above`, which under v2 reads the blocks' present values from those same tables
  (`populate_bundle_state_hashed`; v1 reads the plain state, which is still unreverted). So the
  returned `Chain` holds the pre-unwind values as its present state. The tables it leaves are
  right; only the returned `Chain` is wrong. TN never calls it: its unwinds go through
  `remove_block_and_execution_above`, which returns nothing (TN-0113's round trip covers that).
  A fix reads the present state before the hashed tables are unwound, or unwinds them after.
- **A file rotation publishes rows before the database commit.** When a save fills a static
  file, `increment_block` commits it (`StaticFileProviderRW::commit`) and the reader index names
  its rows at once, while `save_blocks` is still running. If the database arm then fails, the
  headers tip (TN's `last_block_number`) reads ahead of the database until the node halts; the
  restart prunes the rows back (`check_consistency`), and re-execution writes the same ones.
  `test_save_blocks_without_db_commit_heals_across_files_v2` (TN-0115) pins this state.
- **A cache miss after a failed save can cache uncommitted rows.** Since TN-0118 every segment
  writes its `.conf` in its own thread, so after a database-arm failure in `save_blocks` the
  `.conf` files on disk name rows the database never committed. A jar loaded on a cache miss
  then holds them until restart. Reads by number or transaction route by the index, and
  `DatabaseProvider`'s range reads clamp to it; only a direct
  `StaticFileProvider::fetch_range_with_predicate` past the index tip returns them. TN has no
  such caller, and a failed save halts the node.
- **The open-time heal cuts committed rows of a damaged plain jar.** Since TN-0130 a prune
  commits the `.conf` before it shrinks a file, and a checksummed (version 2) jar shorter than
  its `.conf` is refused. A plain (version 1) jar keeps upstream's heal, which cuts committed rows
  to match a short offsets or data file, since an older build's interrupted prune leaves exactly
  that. It fails closed for TN (the static files then lag the stage checkpoints, which TN refuses
  at startup), but the cut rows are gone. Refusing it too would need a marker that a jar was
  last pruned by this build.

## RocksDB stubs and storage-v2 plumbing

RocksDB itself is gone (TN-0014), but the zero-size stub types, the provider-factory field and
the `EitherWriter`/`EitherReader` parameters remain. Removing them changes compiled storage code,
so it waits for the storage work (DECISIONS.md D15; ROADMAP.md, "RocksDB step B").

## MDBX fixes belong to the storage work

Upstream MDBX fixes (7bf9241fe, 294e21507, 5fc3a81d3 in `upstream-backports.md`) are tracked for
the tn-lmdb owners (tndb-b until 2026-11-04), who replace MDBX as the reth database (ROADMAP.md,
Phase 3; tn-lmdb is the default backend since 2026-11-09, DECISIONS.md D22).

## The revm-inspectors patch

`patches/revm-inspectors` carries two changes to the revm-inspectors version TN's revm needs
(0.34.3): Boa's recursion limit, which retires when TN's reth moves to a release with upstream's
fix (0.44.2 or later), and the cap on recorded trace steps (TN-0079), which upstream lacks as of
0.44.3 and must be re-ported on every upgrade. `lockfile.py check` fails when either workspace
resolves the registry crate instead of the patch (TN-0085, review R30), and the nested test lane
now really builds against the patch (TN-0080). `patches/revm-inspectors/PATCH.md` has the
details.

## CI build time with the vendored crates

rust-cache does not save path packages inside the repository, so every test lane now rebuilds the
57 vendored crates, which the git dependency had cached, and `vendor-reth-test` compiles reth
four ways. None of this has been timed on a GitHub runner. Before queueing the PR, a dispatch of
`main-compiles.yaml` on the branch times the builds; caching the vendored crates would need
sccache or a different cache layout.

## The upstream survey is from 2026-11-06

`upstream-backports.md` surveyed upstream `main` up to `44a74f9ea`. The next survey starts there
(CLAUDE.md, "Tracking upstream reth").

## Branches that carry tndb-b [closed 2026-11-09]

The tndb-b work (renamed tn-lmdb on 2026-11-04) was written against an earlier copy of this
vendoring on another branch. `main`'s vendoring (#1768) was merged into it (f88cb29d6), not
rebased, and `sls_storage_v2` builds on that merge. The merge re-added its workspace
dependencies (`reth-db-api`, `reth-storage-errors`, `reth-trie-common`) to the root `Cargo.toml`,
kept `patches/revm-inspectors` and the nested workspace's patch for it (since TN-0080 the
`[patch.crates-io]` entry in `vendor/reth/.cargo/config.toml`), and took the union of both sides'
CI lanes. tn-lmdb is the default reth backend there (DECISIONS.md D22).
