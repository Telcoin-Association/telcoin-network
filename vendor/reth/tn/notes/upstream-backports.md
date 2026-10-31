# Upstream fixes: applied and tracked

The tracker for upstream reth fixes that matter to TN but came after the pin (`tn/upstream.toml`).
Survey: 2026-10-30, upstream `v1.11.4` → `origin/main` at `84caec584`, over the vendored paths
(`git log --no-merges 564ffa586..origin/main`, 1126 commits, about 100 trial-picked). Security
items were re-checked against TN's code on 2026-10-31. reth publishes no GitHub security
advisories, so this commit-log survey is the only source.

To apply one: `RETH_UPSTREAM=<clone> python3 -I vendor/reth/tn/vendor.py pick <sha>`, resolve,
add a CHANGES.md entry (kind `backport`), build, and run the crate's tests with
`vendor/reth/tn/cargo.sh test -p <crate>`. Move the row to "Applied". Check with
`tn/cargo.sh check --workspace`, not `-p <crate>` alone: several vendored crates only build
with the features the workspace unifies.

Severity below is for TN, which differs from upstream's in places. Relevant TN facts:

- consensus and RPC share one tokio runtime, and the execution engine (`crates/engine`,
  `spawn_blocking_task`) and certificate validation (`spawn_blocking`) use the same blocking pool
  as RPC;
- `debug` and `trace` are opt-in (off by default, warned when enabled; `tn-reth/src/cli.rs`);
- non-critical task panics are ignored by TN's `TaskManager`, and `panic` is the default
  `unwind`;
- TN's base fee never goes below `MIN_PROTOCOL_BASE_FEE` (7 wei).

## Applied

| sha | TN id | fix | verification |
|---|---|---|---|
| 997af404a | TN-0015 | trim spaces in CORS domain lists (prerequisite of ff2657600) | upstream unit test |
| ff2657600 | TN-0016 | **security**: enforce `--ws.origins` on WebSocket upgrades (cross-site WS hijacking) | upstream test; fails with the validator disabled |
| 57825d41d | TN-0017 | **security**: JS tracers honor `timeout` and stop on drop (OOM DoS, `debug` only). Ported onto revm-inspectors 0.34 (`rpc/src/js_tracer_interrupt.rs`) | 6 tests; the running-trace test runs >60 s with the interrupt disabled |
| 792ee9245 | TN-0018 | **security**: read-only pool lookups no longer allocate sender ids (unbounded memory growth via `eth_getTransactionCount(.., "pending")`) | upstream test; txpool 222/222 |
| b7a7a8a72, f2d2bd233, de401d9b4, 1f94488d1, e63ec720a, e88a669b1 | TN-0019 | **security**: RPC work on the blocking pool is bounded by its semaphores (`eth_callMany`, `eth_simulateV1`, `eth_getLogs` scans), permits stay held when a client disconnects (`eth_call`, `eth_callMany`, `eth_simulateV1`, `eth_getProof`), and cancelled tasks stop early. High for TN because of the shared blocking pool (above) | upstream test for the getLogs permit; nested workspace builds |

## Open

### High (correctness, not security)

| sha | subject | crate | why it matters to TN | pick |
|---|---|---|---|---|
| 47544d9a7 | add to pending subpool in nonce order | transaction-pool | Out-of-order delivery can put nonce gaps into batches, because the batch builder iterates a live `best_transactions()`. **Recommended next.** | clean |
| 7bf9241fe | disable read-tx timeout during check_consistency | provider | TN runs `check_consistency` at startup; a long heal on an archive DB can hit the MDBX reader timeout and crash-loop. Storage crate: coordinate with the tndb-b work. | 1 import-only conflict; likely clean after 294e21507 |
| 95679ace2 | propagate static-file read errors | db, provider | Corrupt static-file reads return `Ok(None)` ("missing block"). Storage crate: coordinate with tndb-b. | 1 trivial conflict (cursor.rs), rest test-only |
| 22e3708b2 | nippy-jar: end last value at its offset | nippy-jar, provider | Reading the tip row while persistence appends returns over-long bytes, or panics on a stale mmap. Storage: coordinate with tndb-b. | test-only; likely clean after 0d1324f14 |

### Medium

| sha | subject | crate | why | pick |
|---|---|---|---|---|
| d3d7fb31d | ceiling division for replacement price bump | transaction-pool | Below 10 wei the 10% bump rounds to 0, so equal-price replacements are accepted. At TN's 7-wei floor replacements are nearly free with or without it, and the overflow it guards is unreachable (the balance check caps fees far below it). Correctness. **Recommended**: tiny and clean. | clean |
| 13a59de4b (+ 621f663ed) | stop long blocking jobs once the request is dropped | rpc-eth-api, tasks | Extends TN-0019 to synchronous jobs that never yield (e.g. long `eth_call`s). Needs the `CancelOnDrop` move into reth-tasks first. | not tried |
| 6d718d0c2 | use the configured limit in `trace_filter` | rpc | `trace` is opt-in. | not tried |
| 6e4009eed | no panic in log subscription on broadcast lag | rpc | A lagging `eth_subscribe("logs")` client panics its own subscription task. TN ignores non-critical task panics, so only that client's subscription ends (with a panic in the log). | clean |
| 0d1324f14 | handle uncommitted rows in prune_rows | nippy-jar | The static-file heal can leave a config claiming rows whose offsets are not on disk. Storage: coordinate with tndb-b. | clean |
| b629142d0 | preserve refreshed static file cache entries | provider | A concurrent reader can overwrite a freshly refreshed jar with a stale one. Storage. | clean |
| 294e21507 | heal finalized/safe ahead of highest header | provider | After an unwind, "finalized" can point past the tip. Storage. | clean |
| 5fc3a81d3 | bound safe-no-sync rollback with a sync period | db | Without a period, `--db.sync-mode safe-no-sync` can lose days of data on power loss. | test-only |
| bf1db83ae | release receiver lock before validating | transaction-pool | `additional_validation_tasks` validates one transaction at a time. | clean |
| 080a9cfc1, c4cd5c9b7 | `apply_pre_execution_changes` in replay and `debug_traceCallMany` | rpc-eth-api, rpc | TN's EIP-4788/EIP-2935 system calls are skipped in replays and traces. | clean; check `executor_for_block` |
| 8861e2724 | notify subscribers when set_block_info promotes | transaction-pool | TN calls `set_block_info`; transactions promoted by a basefee drop never reach pending listeners. | clean |
| 5b0120dbb | lower default block/receipt cache limits | rpc-eth-types | High default memory use unless TN sets its own limits. | clean |
| 5868ee478 | rewind backward overlay seeks | trie | Overlay cursors skip entries on a backward seek. Not hit by current `StateRoot` callers, but this code sits under TN's output_overlay. | conflicts; hand port is about 8 lines using `reset()` |

### Low

0e14f1a8a and a68f9e19f (IPC loop spin; max subscriptions per connection), 10d130182 (pool gas
limit at startup), 62808bd06 (empty code hash senders), e7ebeb869 (partial receipts), 1a6ba945a
(codecs decode), 078c22d5f (iterative BlockState drop). Each picks cleanly or with test-only
conflicts.

### Not applicable

fd1c9982c (implicit simulate gas cap; Amsterdam/EIP-8037 only), 7b3432d95 (`eth_simulateV1`
timestamp overflow in `sanitize_chain`, which TN's v1.11 simulate code predates), 0eeb8c5ef
(eth/68 packing; devp2p, not vendored), a9b6969e7 (init-state OOM; TN does not use reth's
state-dump import).

## Not simple backports

- **IPC hardening** (95bdbe747, ed9bb9d43, c3d46f3d0) sits on a later codec refactor. Take it at
  the next sync, or default validators to `--ipcdisable`.
- **estimateGas cap** (2bd45d119) needs `rpc-eth-types/settings.rs`, which TN doesn't carry.
- **Trie storage-deletion fixes** (cb7fc64c7, 8617e42f9, 839b84027) are tied to upstream's removal
  of storage-wipe markers (c9b466c13).
- Many trie and storage fixes target code TN doesn't run: proof v2, ArenaParallelSparseTrie,
  RocksDB, storage v2, BAL, Amsterdam.
- **For the next sync:** upstream `9d1638f32` (2026-10-24) replaces revm with "evm2". Since v2.0.0,
  primitives-traits, codecs and zstd-compressors live outside the reth repository, so their
  fixes need separate tracking. When revm-inspectors reaches 0.44.2, TN-0017's port is replaced by
  upstream's `JsInspector::with_interrupt`.

## TN-side hardening (not upstream fixes)

- **A separate blocking pool for RPC.** The root cause behind TN-0019's severity: reth RPC and
  TN's consensus/execution share tokio's blocking pool (512 threads by default). Semaphores bound
  most RPC work, but `eth_estimateGas` and `eth_createAccessList` take no blocking-IO permit (the
  same as upstream main), and debug/trace permits are released when the client disconnects (also
  the same as upstream main, though `debug`/`trace` are opt-in). Giving the RPC `TaskSpawner`
  (`tn-types` `task_manager.rs`) its own runtime or bounded pool would isolate block execution
  from RPC load regardless of per-method permits.
