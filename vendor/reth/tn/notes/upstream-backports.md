# Upstream fixes: applied and tracked

The tracker for upstream reth fixes that matter to TN but came after the pin (`tn/upstream.toml`).
Survey: 2026-10-30, upstream `v1.11.4` → `origin/main` at `84caec584`, over the vendored paths
(`git log --no-merges 564ffa586..origin/main`, 1126 commits, about 100 trial-picked). Security
items were re-checked against TN's code on 2026-10-31, and the static-file paths (`nippy-jar`,
`providers/static_file`, `db/src/static_file`) were swept again the same day. reth publishes no
GitHub security advisories, so this commit-log survey is the only source. A second survey on
2026-11-06 covered `84caec584..44a74f9ea` (26 commits on the vendored paths) and re-checked every
Open row with `git apply --check`; the next survey starts at `44a74f9ea`.

To apply one: `RETH_UPSTREAM=<clone> python3 -I vendor/reth/tn/vendor.py pick <sha>`, resolve,
add a CHANGES.md entry (kind `backport`), build, and run the crate's tests with
`vendor/reth/tn/cargo.sh test -p <crate>`. Move the row to "Applied". Check with
`tn/cargo.sh check --workspace`, not `-p <crate>` alone: several vendored crates only build
with the features the workspace unifies.

Severity below is for TN, which differs from upstream's in places. Relevant TN facts:

- consensus and RPC share one tokio runtime. Until 2026-11-06 the execution engine
  (`crates/engine`, `spawn_blocking_task`) and certificate validation (`spawn_blocking`) also used
  the same blocking pool as RPC; since then RPC's blocking work has a pool of its own
  (`tn-reth/src/rpc_blocking.rs`), and the RPC limits keep their value as per-method bounds;
- `debug` and `trace` are opt-in (off by default, warned when enabled; `tn-reth/src/cli.rs`);
- non-critical task panics are ignored by TN's `TaskManager`, and `panic` is the default
  `unwind`;
- TN's base fee never goes below `MIN_PROTOCOL_BASE_FEE` (7 wei);
- TN runs storage v1 (`StorageSettings::base()`) and never prunes: headers, transactions and
  receipts are in static files; senders, change sets and history indices are in MDBX
  (`provider/src/either_writer.rs`). Until 2026-11-04 this line said receipts were in MDBX; the
  verdicts below that rest on it (1b20c9bfc, 6bd6bf28a) were rechecked by the 2026-11-01 review
  and still hold;
- the tndb-b work replaces MDBX and RocksDB, so fixes to those paths are tracked for its owners.
  Static-file fixes are in scope here.

## Applied

| sha | TN id | fix | verification |
|---|---|---|---|
| 997af404a | TN-0015 | trim spaces in CORS domain lists (prerequisite of ff2657600) | upstream unit test |
| ff2657600 | TN-0016 | **security**: enforce `--ws.origins` on WebSocket upgrades (cross-site WS hijacking) | upstream test; fails with the validator disabled |
| 57825d41d | TN-0017 | **security**: JS tracers honor `timeout` and stop on drop (OOM DoS, `debug` only). Ported onto revm-inspectors 0.34 (`rpc/src/js_tracer_interrupt.rs`). Compiled only into builds with TN's opt-in `js-tracer` feature (D21) | 6 tests; the running-trace test runs >60 s with the interrupt disabled |
| 792ee9245 | TN-0018 | **security**: read-only pool lookups no longer allocate sender ids (unbounded memory growth via `eth_getTransactionCount(.., "pending")`) | upstream test; txpool 222/222 |
| b7a7a8a72, f2d2bd233, de401d9b4, 1f94488d1, e63ec720a, e88a669b1 | TN-0019 | **security**: RPC work on the blocking pool is bounded by its semaphores (`eth_callMany`, `eth_simulateV1`, `eth_getLogs` scans), permits stay held when a client disconnects (`eth_call`, `eth_callMany`, `eth_simulateV1`, `eth_getProof`), and cancelled tasks stop early. High for TN because of the shared blocking pool (above) | upstream test for the getLogs permit; nested workspace builds |
| 47544d9a7 | TN-0022 | pending subpool receives transactions in nonce order, so the batch builder's live `best_transactions()` cannot see a nonce gap | upstream tests; 2 of 3 fail without the fix (gap fill was already right) |
| b629142d0, 01d039b1f | TN-0023 | static-file jar cache: a load no longer overwrites a fresher entry, and invalidation is serialized with in-flight loads | upstream tests |
| 1e17affd3 | TN-0023 | startup recovery removes transaction row 0 when the checkpoint is TN's empty genesis | upstream test fails without the fix |
| 0d1324f14, 22e3708b2 | TN-0023 | nippy-jar: `prune_rows` with uncommitted rows; the last value ends at its offset (tip reads during appends, no panic on a stale mmap) | upstream tests fail without the fixes |
| 95679ace2 | TN-0023 | static-file read errors are errors, not `Ok(None)` ("missing block") | upstream test (adapted) fails without the fix |
| d3d7fb31d | TN-0024 | the replacement price bump rounds up, so below 10 wei an equal-price replacement is refused. Pool admission only; no batch or block validity change | upstream test fails without the fix |
| d85d9deae | TN-0034 | a `syncing` subscription ends when its client leaves (two leaked tasks per abandoned subscription, woken on every block; WS and IPC). Missed by the 2026-10-30 survey, found by the 2026-11-01 review | upstream tests (adapted); both fail without the fix |
| 055fafc72 | TN-0035, TN-0058 | **security**: parallel `eth_getLogs` receipt chunks take their own blocking-IO permit, else run inline; one scan no longer occupies up to six blocking threads (TN-0035). The window-by-window scan rewrite in the same commit is ported as TN-0058: a scan holds one 1,000-header window instead of every matching header of the range. Found by the reviews | 2 TN tests; the permit test fails without the fix; upstream's `test_range_scan_across_windows` |
| revm-inspectors a5638ca (#548) | `patches/revm-inspectors` | **security**: Boa's recursion limit 10,000 → 128, so a recursive getter or setter in a JS tracer cannot overflow a blocking thread's stack and abort the node (`js-tracer` builds only). Carried as a patched crate, not a vendored file; see its PATCH.md. Found by the review | 3 tests; getter and setter abort the process without the fix |
| 6e4009eed | TN-0039 | a lagging `eth_subscribe("logs")` subscriber skips the missed blocks instead of panicking its subscription task | nested build; no upstream test |
| 6d718d0c2 | TN-0040 | `trace_filter`'s range error names the configured limit, not 100 | tn-reth namespace test |
| bf1db83ae | TN-0041 | pool validation workers no longer hold the job queue's lock while validating, so `--txpool.additional-validation-tasks` runs in parallel | upstream test; txpool 228/228 |
| 8861e2724 | TN-0042 | transactions a lower base fee promotes in `set_block_info` reach pending listeners (`newPendingTransactions`) | TN test fails without the fix; txpool 229/229 |
| 62808bd06 | TN-0043 | the pool admits a sender whose code hash is the empty hash (an EOA to the EVM) | upstream test; txpool 230/230 |
| 0e14f1a8a | TN-0044 | an IPC connection's service loop ends when its response channel closes instead of spinning | reth-ipc 17/17 |
| a68f9e19f | TN-0044 | `--rpc.max-subscriptions-per-connection` limits IPC connections (each request had built a fresh counter) | upstream test; reth-ipc 18/18 |
| 9580b7fc1 | TN-0045 | test only: zstd 0.13 compatibility fixtures for nippy-jar, so a zstd bump cannot silently stop reading static files | the test itself |
| 329fcfc73 (partial) | TN-0046 | **security**: an `eth_callBundle` request's transactions share its block gas limit, so one request cannot run N × the call gas cap. Without the EIP-8037 branch and the cancellation check | TN tests; the refusal test fails without the check |
| 5b0120dbb | TN-0047 | RPC cache defaults 1000 blocks and 500 blocks' receipts (were 5000 and 2000; a TN block can be 1 MB) | hand port; nested tests |
| 8fa539225 | TN-0048 | traces and replays apply the node's own block executor's pre-execution changes (TN's EIP-4788 runs only on an output's first batch), not reth's Ethereum `SystemCaller` | tn-reth trace tests through TN's executor |
| c4cd5c9b7 | TN-0048 | `debug_traceCallMany` at a transaction index applies the block's pre-execution changes before replaying its earlier transactions | nested reth-rpc tests |
| 080a9cfc1 | TN-0048 | `spawn_replay_transaction` applies the block's pre-execution changes (unused by TN today) | nested tests |
| 13a59de4b, 621f663ed (partial), 0973a1387, 34f77be92, 329fcfc73 (its check) | TN-0049 | **security**: a dropped RPC request cancels its blocking task, and long loops (call, callMany, estimate, simulate, bundle, replays, traces, pending block) stop between steps. TN scopes `spawn_blocking_io_fut` too, where its replays and traces run; reth-revm's `CancelOnDrop` copy stays | upstream tests + a TN test that fails without the TN scope |
| 33b785d97 | TN-0050 | an empty revert returns `0x` as its error data, as geth does | upstream test |
| 44a74f9ea | TN-0051 | `eth_fillTransaction`, `eth_sendTransaction` and `eth_signTransaction` refuse a request that mixes legacy and EIP-1559 fee fields | nested and tn-reth tests |
| 078c22d5f | TN-0052 | `BlockState` drops its parent chain iteratively (no stack overflow on a long unpersisted chain); Drop only | upstream test |
| 1a6ba945a | TN-0052 | `AlloyHeader::from_compact` returns the buffer past the header; headers TN stores decode as before | codecs tests |
| e7ebeb869 | TN-0052 | `receipts_by_block` returns `None` instead of a partial list for a block missing receipts | upstream tests |
| d46664fc8 | TN-0059 | `eth_simulateV1` loads the state of the block it resolved, not a second resolution of `latest` | existing simulate tests; no upstream test |
| f12d52de2 | TN-0060 | `--txpool.max-batch-size 0` is refused at parse time (it parked the transaction batcher for good) | upstream test |
| 5b6863039 | TN-0062 | a priced call with no `gas` field stays within `--rpc.gascap` (the caller allowance was clamped to the block limit only) | upstream test |
| dfcb10454 | TN-0063 | `eth_getTransactionBySenderAndNonce` returns `null`, not a header error, when the sender's nonce moved without a transaction of theirs in the block (the mock hunk came with TN-0036) | upstream tests |
| 48df75d88 | TN-0064 | `debug_trace*` lookups of a missing block or transaction, and tracing genesis, fail with -32000 as geth does (prerequisite of 5d6634a6f) | upstream test |
| 5d6634a6f | TN-0065 | `debug_traceCall` at an unknown block fails with -32000 like the other `debug_trace*` methods | upstream test |
| 8760eb000 | TN-0066 | `trace_get` selects the frame whose `traceAddress` equals the list (was a flat index into `trace_transaction`) | upstream test |
| 3dd806a43 | TN-0067 | `trace_replayTransaction` returns `transactionHash` with the traces, the shape `trace_replayBlockTransactions` already used | upstream test |
| e3d7f0952 | TN-0068 | `trace_replayTransaction` answers `null` for an unknown transaction (was `-32001`), like `trace_transaction` | upstream test |
| d8d704cf7 | TN-0069 | `trace_callMany` with no block runs on `latest`, not `pending` | upstream test |
| 074bf32ef | TN-0070 | `trace_filter` with no `fromBlock` starts at the latest block, not genesis, so an empty filter no longer fails the range check | upstream test |
| 6d117042f | TN-0071 | `trace_replayBlockTransactions` fills `vmTrace.code` as the single-transaction replay does; adapted to revm-inspectors 0.34 (no `into_trace_results_with_state_parts`) | upstream test |
| 4e1a5574e | TN-0072 | `trace_replayBlockTransactions` fails with `-32001` on an unknown block instead of answering `null` | upstream smoke test |
| 0c8b1c187 | TN-0073 | transactions a parked insertion promotes to pending are announced to the pool's listeners | upstream tests |
| 0a1d3e761 | TN-0074 | the validation job channel holds two jobs per worker instead of one; applied in `build_with_tasks`, the vendored path | upstream test, adapted |
| fe01db183 | TN-0075 | validation submitters send into the bounded channel without a producer mutex; a cancelled submit no longer leaks `inflight_validation_jobs`; hand port, no `spawn` in this copy | upstream tests |
| 43a7452b0 | TN-0076 | the `eth_getLogs` retry range in `QueryExceedsMaxResults` ends before the block that overflowed the limit (TN-0058 follow-up) | upstream test; TN-0058 test asserts the new bound |
| 8f5efd2b3 | TN-0081 | `TrieWalker::new` returns its first read's error instead of panicking the state root computation; roots unchanged | upstream tests; trie suites |
| 345fbbbfd, 75c327ee5, 0720dcd37, 0a21d223a | TN-0102 to TN-0105 | trie overlay cursors skip the database seek on exact overlay hits (`seek_exact`, then `seek` and the hashed post-state cursor), forward in-memory seeks take a fast path, and the state root reuses its storage cursors across accounts. Performance on the state-root path under TN's output overlay | reth-trie, reth-trie-db, tn-reth `trie_overlay`; exec-bench chain differential 0 mismatches; database trie seeks about halved, storage cursor opens 1.89M to 640 per 320 roots |
| 8572c88fe | TN-0140 | **consensus fix**: the trie node iterator no longer skips hashed entries between a seek prefix and a deeper stored branch, which gave a wrong incremental state root. Changes the computed root where the old code was wrong; adiri needs a replay check before it deploys (TN-0140) | upstream test fails without the fix; reth-trie, reth-trie-db, `trie_overlay`; format-freeze block hashes unchanged |
| 8d97ab63c, 1b09bf5a2, 3fba6ad70 | TN-0141 | `StoredNibblesSubKey` encodes to a stack `[u8; 65]` (one allocation less per storage-trie seek and write, same bytes); the storage-root span is trace-level and records its root lazily. Performance on the state-root path | upstream tests (array equals `to_compact` at every length); trie and db-api suites; format-freeze writer bytes unchanged |
| 3fa855645 | TN-0142 | test only: `hash_builder_state_roundtrip` draws `HashBuilderState` from per-field strategies, not the arbitrary interop whose buffer ran out (about one aborted run in ten with `--features arbitrary`); the `state.rs` half, `e2e-test-utils` is not vendored | 50 runs in a row; reth-trie-common suite |

## Open

### Execution path: a coordinated upgrade, not a backport

| sha | subject | crate | why it matters to TN | status |
|---|---|---|---|---|
| 5868ee478 | rewind backward overlay seeks | trie | Overlay cursors skip entries on a backward seek. Not hit by current `StateRoot` callers, but this code sits under TN's output_overlay. | tried 2026-11-09 (312a29e1e, reverted): the clean pick is wrong here, since upstream's `reset()` follows c9b466c13 (no wipe markers) and on this base revives wiped storage (fails reth-trie-db `wiped_storage_is_discarded`); a wipe-keeping `rewind()` passes, but rewinding on any seek below the last returned key doubles the root's database hashed seeks, because `TrieNodeIter` re-seeks between the previous entry and its last `next()` result. Port later so it rewinds only below the cursor's position, or with upstream's node iterator |

### Deferred to its own change

| sha | subject | crate | why it matters to TN | status |
|---|---|---|---|---|
| 75d587208 | `enforce_tracked_nonce` | transaction-pool | The pool re-admits a nonce from a batch that reached quorum, so a sender gets free skipped transactions (also on the reth git dependency). Enabling it needs the batch builder's repool, requeue and orphan paths reconciled first. Pool admission only: no batch or block validity change | described in `open-issues.md` |

### High (correctness, not security)

| sha | subject | crate | why it matters to TN | pick |
|---|---|---|---|---|
| 7bf9241fe | disable read-tx timeout during check_consistency | provider | TN runs `check_consistency` at startup; a long heal on an archive DB can hit the MDBX reader timeout and crash-loop. **MDBX: for the tndb-b owners.** | 1 import-only conflict; likely clean after 294e21507 |

### Medium

| sha | subject | crate | why | pick |
|---|---|---|---|---|
| 294e21507 | heal finalized/safe ahead of highest header | provider | After an unwind, "finalized" can point past the tip. Lives in the MDBX `DatabaseProvider`: **for the tndb-b owners.** | clean |
| 5fc3a81d3 | bound safe-no-sync rollback with a sync period | db | Without a period, `--db.sync-mode safe-no-sync` can lose days of data on power loss. **MDBX: for the tndb-b owners.** | test-only |

### Low

None left.

### Not applicable

fd1c9982c (implicit simulate gas cap; Amsterdam/EIP-8037 only), 7b3432d95 (`eth_simulateV1`
timestamp overflow in `sanitize_chain`, which TN's v1.11 simulate code predates), 0eeb8c5ef
(eth/68 packing; devp2p, not vendored), a9b6969e7 (init-state OOM; TN does not use reth's
state-dump import), 1b20c9bfc (prune checkpoints in the static-file consistency check; senders
and changesets are in MDBX under storage v1, and receipts are in static files but TN never prunes,
so the checkpoints it reads are unset),
6bd6bf28a (receipts static-file sizing; node-builder and receipts-in-static-files only),
00f9bd2a9 (`tx_hash()` instead of `trie_hash()`; equal for Ethereum transaction types, so it only
saves a re-hash),
db17c899c (`reth db migrate-v2`; TN has no reth CLI), adfcad70c (edits a pruned-anchor check
v1.11.4 does not have), 10d130182 (the pool's starting block gas limit from the tip header instead
of 30M; every TN header carries the genesis gas limit, which is 30M on both chains, so nothing
changes).
From the 2026-11-06 survey: 6624ff8a9 (default `eth_call` gas cap; Amsterdam only), b2f6dc910,
c0e8be419, 2a343e40f, 6f7896f9c and 19d390ad9 (evm2, the next sync), 65977b8e1 (secp256k1
bump; a third-party version change on the fork list, and the devp2p crates are not carried),
aa03178bd (`eth_capabilities`, not in v1.11.4), b7a853a60 (`reth` namespace, removed),
9dc3e4c9c, 32d01073f, 8f3cfd117, 824dc1c89 and its revert 14bf073c6 (auth server and JWT; TN
runs no engine API), 277fc1257 (`mev` namespace, removed), 965ba8cb5 (performance only),
d5991a4db, 04f829c26, 0d1b63a1f (cosmetic).

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

- **reth's tests now build against the carried revm-inspectors (TN-0080, 2026-11-09).** cargo
  does not forward `--config` to cargo-nextest, so the `test-vendor-reth` lane had been testing
  the registry crate; the patch now lives in `vendor/reth/.cargo/config.toml`.
- **A separate blocking pool for RPC (done 2026-11-06).** The root cause behind TN-0019's
  severity was that reth RPC and TN's consensus/execution shared tokio's blocking pool (512
  threads by default), and each worker's RPC server bounds its work with its own permits, so the
  bounds add up across workers. `tn-reth/src/rpc_blocking.rs` gives RPC's blocking work a
  runtime of its own (up to 512 threads), through the eth API's and the RPC modules' task
  spawners; every other RPC task keeps its spawner. The per-method permits still bound each
  method: `eth_estimateGas`, `eth_fillTransaction`, `eth_createAccessList` and `eth_callBundle`
  hold a blocking-IO permit (TN-0037 and tn-reth's `rpc_call.rs`), `debug_trace*`, `trace_*` and
  `debug_executionWitness*` keep their tracing permit until their thread ends (TN-0036, TN-0038),
  and a dropped request stops its long loops (TN-0049).
- **`trace_filter` replays bounded by its permits (TN-0077, 2026-11-09).** The filter's two
  permits now also bound the blocks it replays at once; before, a chunk of up to
  `--rpc.max-tracing-requests` blocks traced in parallel on unpermitted blocking threads.
- **Raw `debug_traceBlock` bounded like callMany (TN-0078, 2026-11-09).** The caller's header
  and transactions are bounded by the call count and gas budget of TN-0057; stored
  blocks keep their chain-enforced bounds.
- **A cap on recorded trace steps (TN-0079, 2026-11-09).** The carried revm-inspectors'
  `TracingInspector` stops a transaction whose recorded steps pass 160 MiB (`trace too large`,
  `-32603`), shared across the inspectors of one request. Upstream caps nothing as of
  0.44.3; re-port on every revm-inspectors upgrade (`patches/revm-inspectors/PATCH.md`).
- **A missing bytecode row fails the block (TN-0082, 2026-11-10).** `code_by_hash_ref` no longer
  returns empty code for a hash the `Bytecodes` table lacks; roots unchanged for a
  healthy database.
- **A torn first launch resumes (TN-0083, 2026-11-10).** Genesis init with the header in static
  files and an empty database writes the MDBX half instead of failing forever.
